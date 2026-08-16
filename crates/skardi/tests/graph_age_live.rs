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

use skardi::sources::hierarchy::HierarchyLevel;
use skardi::sources::providers::graph::client::{AgeClient, GraphClient, QueryBounds};
use skardi::sources::providers::graph::config::GraphConfig;
use skardi::sources::providers::graph::error::GraphError;
use skardi::sources::providers::graph::udtf::{GraphSourceHealth, GraphSources};
use skardi::sources::providers::graph::{
    register_graph_source, register_graph_tables, register_graph_udtfs,
};
use skardi::util::json_getters::register_json_getter_udfs;

fn live_url() -> Option<String> {
    let url = std::env::var("SKARDI_AGE_LIVE_URL")
        .ok()
        .filter(|u| !u.trim().is_empty());
    // CI arms this: absent gating env is a SKIP for a developer's
    // laptop but a hard failure where the suite is the AGE backend's
    // ONLY coverage — a renamed or dropped URL var must not turn nine
    // skips into a green step nobody reads.
    assert!(
        url.is_some() || std::env::var("SKARDI_AGE_LIVE_REQUIRED").is_err(),
        "SKARDI_AGE_LIVE_REQUIRED is set but SKARDI_AGE_LIVE_URL is not — \
         the AGE backend would have gone untested"
    );
    url
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
                   (d:Person {{name: '颱風', note: 'café ☔'}}),
                   (a)-[:KNOWS {{since: 2019}}]->(b),
                   (b)-[:KNOWS {{since: 2021}}]->(c),
                   (a)-[:KNOWS {{since: 2024}}]->(d)
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

/// Split a maybe-credentialed URL into (cred-free URL, user, pass):
/// config validation rejects URL-embedded passwords, so the registered
/// source takes credentials the designed way — env-var NAMES.
fn split_creds(url: &str) -> (String, Option<String>, Option<String>) {
    let mut parsed = url::Url::parse(url).expect("live URL parses");
    let user = (!parsed.username().is_empty()).then(|| parsed.username().to_string());
    let pass = parsed.password().map(str::to_string);
    parsed
        .set_username("")
        .expect("postgres URLs take userinfo");
    parsed
        .set_password(None)
        .expect("postgres URLs take userinfo");
    (parsed.to_string(), user, pass)
}

/// YAML `username_env`/`password_env` lines for a URL's credentials,
/// with the values exported under `prefix`-derived env names.
fn cred_lines(url: &str, prefix: &str) -> String {
    let (_, user, pass) = split_creds(url);
    let mut lines = String::new();
    if let Some(u) = &user {
        let env = format!("{prefix}_USER");
        unsafe { std::env::set_var(&env, u) };
        lines.push_str(&format!("username_env: {env}\n"));
    }
    if let Some(p) = &pass {
        let env = format!("{prefix}_PASS");
        unsafe { std::env::set_var(&env, p) };
        lines.push_str(&format!("password_env: {env}\n"));
    }
    lines
}

/// A registered source + session, against the live database. Credentials
/// ride env vars (unique per graph name, so parallel tests never race).
async fn live_ctx(url: &str, graph: &str) -> (SessionContext, GraphSources) {
    let sources: GraphSources = Arc::new(RwLock::new(HashMap::new()));
    let (clean_url, user, pass) = split_creds(url);
    let user_env = format!("SKARDI_AGE_LIVE_USER_{}", graph.to_uppercase());
    let pass_env = format!("SKARDI_AGE_LIVE_PASS_{}", graph.to_uppercase());
    let mut cred_lines = String::new();
    if let Some(u) = &user {
        unsafe { std::env::set_var(&user_env, u) };
        cred_lines.push_str(&format!("username_env: {user_env}\n"));
    }
    if let Some(p) = &pass {
        unsafe { std::env::set_var(&pass_env, p) };
        cred_lines.push_str(&format!("password_env: {pass_env}\n"));
    }
    let config: GraphConfig = serde_yaml::from_str(&format!(
        "backend: age\ngraph_name: {graph}\nquery_timeout_seconds: 10\nmax_rows: 100\n{cred_lines}"
    ))
    .expect("config parses");
    register_graph_source(&sources, "kg", &clean_url, &config)
        .await
        .expect("registration connects eagerly");
    let ctx = SessionContext::new();
    register_graph_udtfs(&ctx, Arc::clone(&sources)).expect("udtfs register");
    // The getter family is the session's registration, not the graph
    // UDTFs' (register_graph_udtfs' doc) — register_json_getter_udfs
    // installs the UDFs without the `->` operator rewrite.
    register_json_getter_udfs(&ctx).expect("json getters register");
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
        // CI's coverage job runs `-- --ignored` across the board (the
        // documents_s3_live convention): absent gating env means SKIP,
        // loudly on stderr — never a panic.
        eprintln!("skipping live AGE test: set SKARDI_AGE_LIVE_URL to run");
        return;
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

    // A NULL property lands as SQL NULL (cyd has no age), and the CJK
    // name round-trips byte-faithfully (the mojibake failure mode:
    // Latin-1 per-byte push would deliver a corrupted-but-parseable
    // string, so this MUST assert the exact value end to end).
    let batches = collect(
        &ctx,
        "SELECT name, age FROM cypher_query('kg', '\
             MATCH (p:Person) RETURN p.name, p.age', '{}', \
         '{\"name\": \"string\", \"age\": \"int\"}') ORDER BY name",
    )
    .await;
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 4);
    let all_names: Vec<String> = batches
        .iter()
        .flat_map(|b| {
            let col = b.column(0).as_any().downcast_ref::<StringArray>().unwrap();
            (0..b.num_rows())
                .map(|i| col.value(i).to_string())
                .collect::<Vec<_>>()
        })
        .collect();
    assert!(
        all_names.contains(&"颱風".to_string()),
        "CJK survives byte-faithfully: {all_names:?}"
    );

    drop_graph(&pool, &graph).await;
}

#[tokio::test]
#[ignore = "needs a live Postgres+AGE (set SKARDI_AGE_LIVE_URL); see module doc"]
async fn nodes_relationships_and_paths_take_the_canonical_shapes() {
    let Some(url) = live_url() else {
        // CI's coverage job runs `-- --ignored` across the board (the
        // documents_s3_live convention): absent gating env means SKIP,
        // loudly on stderr — never a panic.
        eprintln!("skipping live AGE test: set SKARDI_AGE_LIVE_URL to run");
        return;
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
    assert_eq!(total, 4);
    let names = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(names.value(0), "ada");

    // json_get through a node's properties keeps non-ASCII intact —
    // the whole agtype → JSON-text → json_get chain, live.
    let batches = collect(
        &ctx,
        "SELECT json_get_str(v.properties, 'note') AS note \
         FROM cypher_query('kg', 'MATCH (v:Person {name: \"颱風\"}) RETURN v', \
         '{}', '{\"v\": \"node\"}')",
    )
    .await;
    let notes = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(notes.value(0), "café ☔");

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
    assert_eq!(rels.len(), 3);
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
        // CI's coverage job runs `-- --ignored` across the board (the
        // documents_s3_live convention): absent gating env means SKIP,
        // loudly on stderr — never a panic.
        eprintln!("skipping live AGE test: set SKARDI_AGE_LIVE_URL to run");
        return;
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
        // CI's coverage job runs `-- --ignored` across the board (the
        // documents_s3_live convention): absent gating env means SKIP,
        // loudly on stderr — never a panic.
        eprintln!("skipping live AGE test: set SKARDI_AGE_LIVE_URL to run");
        return;
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

    // The row cap: max_rows 1 fails LOUDLY (typed, names the cap),
    // never a silent truncation — and the ERROR path must not leak its
    // PREPARE onto the pooled connection (prepared statements are
    // session-level; rollback does not clear them).
    let sources: GraphSources = Arc::new(RwLock::new(HashMap::new()));
    let (clean_url, _, _) = split_creds(&url);
    let creds = cred_lines(&url, "SKARDI_AGE_CAPPED");
    let config: GraphConfig = serde_yaml::from_str(&format!(
        "backend: age\ngraph_name: {graph}\nmax_rows: 1\n{creds}"
    ))
    .expect("config parses");
    register_graph_source(&sources, "capped", &clean_url, &config)
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
        .expect_err("4 people, cap 1");
    let msg = err.to_string();
    assert!(msg.contains("max_rows = 1"), "{msg}");
    assert!(msg.contains("LIMIT"), "the fix is named: {msg}");

    // …and taking the error message's own advice works: a SQL LIMIT at
    // the cap consumes exactly that many rows and stops CLEANLY — the
    // limit is pushed to the consumption side, it never trips the cap.
    let batches = collect(
        &capped,
        "SELECT name FROM cypher_query('capped', \
         'MATCH (p:Person) RETURN p.name', '{}', '{\"name\": \"string\"}') LIMIT 1",
    )
    .await;
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 1);

    // Leak regression: hammer the capped (error) path well past the
    // pool size — parameterized, so each call PREPAREs — then sweep the
    // SAME pool's sessions for leftover skq_p_* statements
    // (pg_prepared_statements is session-local, so the sweep must run on
    // the very sessions execute used; hence the direct AgeClient and its
    // test-only pool hook). Before the fix each error path left one
    // behind, monotonically.
    let (clean_url, user, pass) = split_creds(&url);
    unsafe {
        if let Some(u) = &user {
            std::env::set_var("SKARDI_AGE_LEAK_USER", u);
        }
        if let Some(p) = &pass {
            std::env::set_var("SKARDI_AGE_LEAK_PASS", p);
        }
    }
    let client = AgeClient::connect(
        "leakcheck",
        &clean_url,
        &graph,
        user.as_ref().map(|_| "SKARDI_AGE_LEAK_USER"),
        pass.as_ref().map(|_| "SKARDI_AGE_LEAK_PASS"),
        4,
        std::time::Duration::from_secs(10),
    )
    .await
    .expect("connects");
    let tight = QueryBounds {
        timeout: std::time::Duration::from_secs(10),
        max_rows: 1,
    };
    // CONCURRENT, so the pool genuinely opens max_connections sessions —
    // sequential execute() reuses the one idle connection
    // (min_connections defaults to 0) and a "sweep" would only ever see
    // a single session.
    let hammer_params = serde_json::json!({"x": "nobody"});
    let results = futures::future::join_all((0..8).map(|_| {
        client.execute(
            "MATCH (p:Person) WHERE p.name <> $x RETURN p.name",
            &hammer_params,
            1,
            tight,
            None,
        )
    }))
    .await;
    for result in results {
        let err = result
            .err()
            .expect("4 people, cap 1: the capped error path");
        assert!(err.to_string().contains("max_rows = 1"), "{err}");
    }
    // BACKEND error paths leak differently from client-local ones: a
    // runtime error aborts the transaction, where DEALLOCATE itself is
    // refused until ROLLBACK. Parameterized (so the
    // PREPARE exists and the EXECUTE fails at runtime), concurrent for
    // the same session-fan-out reason as above.
    let div_params = serde_json::json!({"x": 1});
    let results = futures::future::join_all((0..8).map(|_| {
        client.execute(
            "MATCH (p:Person) RETURN $x / 0",
            &div_params,
            1,
            tight,
            None,
        )
    }))
    .await;
    for result in results {
        assert!(result.is_err(), "division by zero is a backend error");
    }
    // Sweep EVERY pooled session, not whichever one a lone query lands
    // on: acquire all max_connections connections and hold them while
    // checking each — that is what makes "no session carries a leak"
    // true rather than assumed.
    let conns = futures::future::join_all((0..4).map(|_| client.pool_for_tests().acquire())).await;
    for conn in conns {
        let mut conn = conn.expect("acquire pooled session");
        let leaked: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM pg_prepared_statements WHERE name LIKE 'skq_p_%'",
        )
        .fetch_one(&mut *conn)
        .await
        .expect("pg_prepared_statements readable");
        assert_eq!(
            leaked, 0,
            "no skq_p_* statement survives on ANY pooled session"
        );
    }

    // Hostile-looking parameter VALUES round-trip inertly through the
    // serde_json + quote-doubling boundary (the design's recorded AGE
    // exception): apostrophes, backslashes, SQL fragments.
    let hostile = r#"O'Brien \ '; DROP TABLE documents; --"#;
    let stream = client
        .execute(
            "RETURN $s",
            &serde_json::json!({"s": hostile}),
            1,
            tight,
            None,
        )
        .await
        .expect("hostile param is inert data");
    use futures::TryStreamExt;
    let rows: Vec<_> = stream.try_collect().await.expect("collects");
    assert_eq!(
        rows[0][0],
        serde_json::json!(hostile),
        "byte-faithful round trip"
    );

    drop_graph(&pool, &graph).await;
}

#[tokio::test]
#[ignore = "needs a live Postgres+AGE (set SKARDI_AGE_LIVE_URL); see module doc"]
async fn duplicate_registration_keeps_the_original_connection() {
    let Some(url) = live_url() else {
        eprintln!("skipping live AGE test: set SKARDI_AGE_LIVE_URL to run");
        return;
    };
    let graph = unique_graph("dup");
    let pool = seed_graph(&url, &graph).await;
    let (ctx, sources) = live_ctx(&url, &graph).await;

    // Second registration under the SAME name (pointing at a different
    // graph) must fail AND leave the original routing untouched.
    let (clean_url, _, _) = split_creds(&url);
    let creds = cred_lines(&url, "SKARDI_AGE_DUP");
    // Same (existing) graph, duplicate connection NAME — the name is
    // what's under test; a nonexistent graph would now trip the
    // registration-time existence probe first.
    let config: GraphConfig =
        serde_yaml::from_str(&format!("backend: age\ngraph_name: {graph}\n{creds}"))
            .expect("config parses");
    let err = register_graph_source(&sources, "kg", &clean_url, &config)
        .await
        .expect_err("duplicate name refuses");
    assert!(err.to_string().contains("already registered"), "{err}");

    // The original connection still serves — 4 people, not an error and
    // not the empty _other graph.
    let batches = collect(
        &ctx,
        "SELECT name FROM cypher_query('kg', 'MATCH (p:Person) RETURN p.name', '{}', \
         '{\"name\": \"string\"}')",
    )
    .await;
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 4);

    drop_graph(&pool, &graph).await;
}

/// YAML views end to end against a REAL AGE backend: registration
/// validates each view's Cypher and contract against the live graph
/// (this suite is the only place that path meets real agtype), the
/// catalog tables answer plain SQL — projection, WHERE, a
/// `nullable: false` assertion, a relationship STRUCT — and a view
/// whose declared arity contradicts its RETURN clause refuses the whole
/// registration, publishing neither handle nor catalog.
#[tokio::test]
#[ignore = "needs a live Postgres+AGE (set SKARDI_AGE_LIVE_URL); see module doc"]
async fn yaml_views_register_and_scan_against_a_live_backend() {
    let Some(url) = live_url() else {
        eprintln!("skipping live AGE test: set SKARDI_AGE_LIVE_URL to run");
        return;
    };
    let graph = unique_graph("views");
    let pool = seed_graph(&url, &graph).await;
    let (clean_url, user, pass) = split_creds(&url);
    unsafe {
        std::env::set_var("SKARDI_AGE_VW_USER", user.unwrap_or_default());
        std::env::set_var("SKARDI_AGE_VW_PASS", pass.unwrap_or_default());
    }

    let config: GraphConfig = serde_yaml::from_str(&format!(
        r#"
backend: age
graph_name: {graph}
username_env: SKARDI_AGE_VW_USER
password_env: SKARDI_AGE_VW_PASS
views:
  - name: people
    cypher: MATCH (p:Person) RETURN p.name, p.age
    schema:
      - {{name: name, type: string, nullable: false}}
      - {{name: age, type: int}}
  - name: knows
    cypher: MATCH (a:Person)-[k:KNOWS]->(b:Person) RETURN a.name, k, b.name
    schema:
      - {{name: src, type: string}}
      - {{name: rel, type: relationship}}
      - {{name: dst, type: string}}
"#
    ))
    .expect("config parses");
    let sources: GraphSources = Arc::new(RwLock::new(HashMap::new()));
    let mut ctx = SessionContext::new();
    register_json_getter_udfs(&ctx).expect("getters");
    register_graph_tables(
        &mut ctx,
        &sources,
        "kg",
        &clean_url,
        Some(&config),
        false,
        HierarchyLevel::Catalog,
    )
    .await
    .expect("both views validate against the live graph");
    {
        let sources = sources.read().unwrap_or_else(|p| p.into_inner());
        let handle = sources.get("kg").expect("handle published");
        let health = handle.health.read().unwrap_or_else(|p| p.into_inner());
        assert!(
            matches!(&*health, GraphSourceHealth::Healthy),
            "a reachable, contract-honoring registration is healthy: {health:?}"
        );
    }

    // Plain SQL over the node view: projection, WHERE, ordering. The
    // nullable:false assertion on `name` held at validation (every
    // seeded person has a name) — `age` is nullable and cyd/颱風 prove it.
    let batches = collect(
        &ctx,
        "SELECT name, age FROM kg.main.people WHERE age IS NOT NULL ORDER BY name",
    )
    .await;
    let names: Vec<String> = batches
        .iter()
        .flat_map(|b| {
            let col = b
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("string");
            (0..b.num_rows())
                .map(|i| col.value(i).to_string())
                .collect::<Vec<_>>()
        })
        .collect();
    assert_eq!(
        names,
        vec!["ada", "bob"],
        "SQL WHERE filtered the null ages"
    );

    // The relationship view: the STRUCT columns carry the canonical
    // shape, and its JSON properties answer the getter family.
    let batches = collect(
        &ctx,
        "SELECT src, rel.\"type\" AS rel_type, json_get_int(rel.properties, 'since') AS since, dst \
         FROM kg.main.knows ORDER BY since",
    )
    .await;
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 3, "the three seeded KNOWS edges");
    let first = &batches[0];
    let rel_type = first
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("type string");
    assert_eq!(rel_type.value(0), "KNOWS");

    // A view whose declared arity contradicts its RETURN clause: the
    // live backend answers, the contract check refuses, the error names
    // the view, and NOTHING is published under the new source name.
    let bad: GraphConfig = serde_yaml::from_str(&format!(
        r#"
backend: age
graph_name: {graph}
username_env: SKARDI_AGE_VW_USER
password_env: SKARDI_AGE_VW_PASS
views:
  - name: lopsided
    cypher: MATCH (p:Person) RETURN p.name, p.age
    schema:
      - {{name: name, type: string}}
"#
    ))
    .expect("config parses");
    let err = register_graph_tables(
        &mut ctx,
        &sources,
        "kg2",
        &clean_url,
        Some(&bad),
        false,
        HierarchyLevel::Catalog,
    )
    .await
    .expect_err("an arity mismatch is a contract violation, not an outage");
    let msg = err.to_string();
    assert!(msg.contains("lopsided"), "the error names the view: {msg}");
    assert!(
        !sources
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .contains_key("kg2"),
        "a refused registration publishes no handle"
    );
    assert!(ctx.catalog("kg2").is_none(), "nor a catalog");

    drop_graph(&pool, &graph).await;
}

/// The server entry (`register_graph_tables`) must hard-fail every
/// registration error that is NOT a connectivity failure: a typo'd
/// graph_name and a wrong password are server-answered contract/config
/// problems — degrading them would let a misconfiguration sail through
/// startup and sit degraded forever.
#[tokio::test]
#[ignore = "needs a live Postgres+AGE (set SKARDI_AGE_LIVE_URL); see module doc"]
async fn server_registration_hard_fails_non_availability_errors() {
    let Some(url) = live_url() else {
        eprintln!("skipping live AGE test: set SKARDI_AGE_LIVE_URL to run");
        return;
    };
    let graph = unique_graph("hardfail");
    let pool = seed_graph(&url, &graph).await;
    let (clean_url, user, pass) = split_creds(&url);
    unsafe {
        std::env::set_var("SKARDI_AGE_HF_USER", user.unwrap_or_default());
        std::env::set_var("SKARDI_AGE_HF_PASS", pass.unwrap_or_default());
        std::env::set_var("SKARDI_AGE_HF_WRONG", "definitely_wrong_pw_9");
    }

    // A typo'd graph_name: the server answered (no such graph) — refused,
    // not degraded.
    let config: GraphConfig = serde_yaml::from_str(&format!(
        "backend: age\ngraph_name: {graph}_misspelled\nusername_env: SKARDI_AGE_HF_USER\npassword_env: SKARDI_AGE_HF_PASS\n"
    ))
    .expect("config parses");
    let sources: GraphSources = Arc::new(RwLock::new(HashMap::new()));
    let mut ctx = SessionContext::new();
    let err = register_graph_tables(
        &mut ctx,
        &sources,
        "kg",
        &clean_url,
        Some(&config),
        false,
        HierarchyLevel::Catalog,
    )
    .await
    .expect_err("a typo'd graph is a configuration error, not an outage");
    let msg = err.to_string();
    assert!(msg.contains("does not exist"), "{msg}");
    assert!(!matches!(err, GraphError::Unavailable { .. }), "{msg}");
    assert!(
        sources.read().unwrap_or_else(|p| p.into_inner()).is_empty(),
        "a refused registration publishes no handle"
    );
    assert!(ctx.catalog("kg").is_none(), "nor a catalog");

    // A wrong password: the server answers 28P01 — also refused.
    let config: GraphConfig = serde_yaml::from_str(&format!(
        "backend: age\ngraph_name: {graph}\nusername_env: SKARDI_AGE_HF_USER\npassword_env: SKARDI_AGE_HF_WRONG\n"
    ))
    .expect("config parses");
    let err = register_graph_tables(
        &mut ctx,
        &sources,
        "kg",
        &clean_url,
        Some(&config),
        false,
        HierarchyLevel::Catalog,
    )
    .await
    .expect_err("an auth failure must not degrade either");
    let msg = err.to_string();
    assert!(msg.contains("password authentication failed"), "{msg}");
    assert!(!matches!(err, GraphError::Unavailable { .. }), "{msg}");
    assert!(
        !msg.contains("definitely_wrong_pw_9"),
        "the credential never echoes: {msg}"
    );

    drop_graph(&pool, &graph).await;
}

#[tokio::test]
#[ignore = "needs a live Postgres+AGE (set SKARDI_AGE_LIVE_URL); see module doc"]
async fn a_typoed_graph_name_fails_registration_not_discovery() {
    let Some(url) = live_url() else {
        eprintln!("skipping live AGE test: set SKARDI_AGE_LIVE_URL to run");
        return;
    };
    // Without the existence probe this split: graph_schema returned ZERO
    // ROWS with no error (an agent reads "empty graph") while
    // cypher_query failed per-query — a typo must fail at registration,
    // named (reproduced live before the probe existed).
    let graph = unique_graph("typo");
    let pool = seed_graph(&url, &graph).await;
    let (clean_url, _, _) = split_creds(&url);
    let creds = cred_lines(&url, "SKARDI_AGE_TYPO");
    let config: GraphConfig = serde_yaml::from_str(&format!(
        "backend: age\ngraph_name: {graph}_misspelled\n{creds}"
    ))
    .expect("config parses");
    let sources: GraphSources = Arc::new(RwLock::new(HashMap::new()));
    let err = register_graph_source(&sources, "kg", &clean_url, &config)
        .await
        .expect_err("nonexistent graph refuses at registration");
    let msg = err.to_string();
    assert!(msg.contains("does not exist"), "{msg}");
    assert!(msg.contains("create_graph"), "the fix is named: {msg}");

    drop_graph(&pool, &graph).await;
}

#[tokio::test]
#[ignore = "needs a live Postgres+AGE (set SKARDI_AGE_LIVE_URL); see module doc"]
async fn the_timeout_bound_is_typed_and_credentials_never_reach_errors() {
    let Some(url) = live_url() else {
        eprintln!("skipping live AGE test: set SKARDI_AGE_LIVE_URL to run");
        return;
    };
    let graph = unique_graph("bounds");
    let pool = seed_graph(&url, &graph).await;

    // 1) The timeout — the row cap's equal-billing sibling — end to end:
    // statement_timeout fires server-side (57014) and surfaces as the
    // TYPED Timeout naming the seconds, not a generic Backend error.
    let (clean_url, user, pass) = split_creds(&url);
    unsafe {
        if let Some(u) = &user {
            std::env::set_var("SKARDI_AGE_BOUNDS_USER", u);
        }
        if let Some(p) = &pass {
            std::env::set_var("SKARDI_AGE_BOUNDS_PASS", p);
        }
    }
    let client = AgeClient::connect(
        "bounds",
        &clean_url,
        &graph,
        user.as_ref().map(|_| "SKARDI_AGE_BOUNDS_USER"),
        pass.as_ref().map(|_| "SKARDI_AGE_BOUNDS_PASS"),
        4,
        std::time::Duration::from_secs(1),
    )
    .await
    .expect("connects");
    let err = client
        .execute(
            // Unbounded cartesian blowup over the seeded graph: 4^14 ≈
            // 268M pattern combinations cannot be counted in 1s — and
            // statement_timeout kills it AT 1s, so the test never waits
            // for the full count either.
            "MATCH (a),(b),(c),(d),(e),(f),(g),(h),(i),(j),(k),(l),(m),(n) RETURN count(*)",
            &serde_json::json!({}),
            1,
            QueryBounds {
                timeout: std::time::Duration::from_secs(1),
                max_rows: 10,
            },
            None,
        )
        .await
        .err()
        .expect("the cartesian traversal cannot finish in 1s");
    let msg = err.to_string();
    assert!(msg.contains("timed out after 1s"), "typed, named: {msg}");

    // 2) graph_schema's own row cap (the labels() branch).
    let err = client
        .labels(
            QueryBounds {
                timeout: std::time::Duration::from_secs(10),
                max_rows: 1,
            },
            None,
        )
        .await
        .expect_err("2 labels, cap 1");
    assert!(err.to_string().contains("max_rows = 1"), "{err}");

    // 3) Credentials never reach error text: force an auth failure and
    // assert neither the wrong nor the real password appears.
    if pass.is_some() {
        unsafe { std::env::set_var("SKARDI_AGE_BADPASS", "wrong-password-on-purpose") };
        let err = AgeClient::connect(
            "badcreds",
            &clean_url,
            &graph,
            user.as_ref().map(|_| "SKARDI_AGE_BOUNDS_USER"),
            Some("SKARDI_AGE_BADPASS"),
            4,
            std::time::Duration::from_secs(5),
        )
        .await
        .expect_err("wrong password refuses");
        let msg = err.to_string();
        assert!(
            !msg.contains("wrong-password-on-purpose"),
            "the credential value never appears in errors: {msg}"
        );
        if let Some(real) = &pass {
            assert!(!msg.contains(real.as_str()), "nor the real one: {msg}");
        }
    }

    drop_graph(&pool, &graph).await;
}

#[tokio::test]
#[ignore = "needs a live Postgres+AGE (set SKARDI_AGE_LIVE_URL); see module doc"]
async fn agtype_float_specials_are_pinned() {
    let Some(url) = live_url() else {
        eprintln!("skipping live AGE test: set SKARDI_AGE_LIVE_URL to run");
        return;
    };
    // Pins whether AGE emits non-JSON float spellings (NaN/Infinity)
    // through agtype_out, and what our decode does with them (the
    // overflow spelling is real: 1.0e308 * 10 emits a bare Infinity
    // token). Whatever the outcome, it must be a
    // PROPORTIONATE per-cell/typed result, not an opaque whole-scan
    // failure with no identity.
    let graph = unique_graph("nan");
    let pool = seed_graph(&url, &graph).await;
    let (ctx, _sources) = live_ctx(&url, &graph).await;

    // Pinned live: sqrt(-1.0) is SQL NULL from AGE itself, while float
    // OVERFLOW emits the bare token `Infinity` through agtype_out — the
    // reachable case the review asked about. Both decode to NULL floats
    // (proportionate; a whole-scan MalformedCell for a legitimate value
    // would be the wrong severity).
    for cypher in ["RETURN sqrt(-1.0)", "RETURN 1.0e308 * 10"] {
        let batches = collect(
            &ctx,
            &format!(
                "SELECT f FROM cypher_query('kg', '{cypher}', '{{}}', \
                 '{{\"f\": \"float\"}}')"
            ),
        )
        .await;
        let col = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Float64Array>()
            .unwrap();
        assert!(col.is_null(0), "{cypher}: a float special is a NULL float");
    }

    drop_graph(&pool, &graph).await;
}

#[tokio::test]
#[ignore = "needs a live Postgres+AGE (set SKARDI_AGE_LIVE_URL); see module doc"]
async fn error_paths_bounds_and_binding_hardening_holds_end_to_end() {
    let Some(url) = live_url() else {
        eprintln!("skipping live AGE test: set SKARDI_AGE_LIVE_URL to run");
        return;
    };
    let graph = unique_graph("r4");
    let pool = seed_graph(&url, &graph).await;
    let (ctx, _sources) = live_ctx(&url, &graph).await;

    // ── #9: a BACKEND error must not echo the caller's Cypher. The
    // query carries a recognizable marker; the rendered error must name
    // the backend failure without the statement context Postgres
    // attaches (db.message(), never sqlx Display or `position` lines).
    let err = ctx
        .sql(
            "SELECT x FROM cypher_query('kg', \
             'MATCH (marker_needle_xyz) RETURN nonexistent_fn(marker_needle_xyz)', \
             '{}', '{\"x\": \"json\"}')",
        )
        .await
        .expect("plans")
        .collect()
        .await
        .expect_err("unknown function is a backend error");
    let msg = err.to_string();
    assert!(
        !msg.contains("marker_needle_xyz"),
        "backend errors never echo query text: {msg}"
    );

    // ── #8: pool saturation is bounded and TYPED. max_connections
    // defaults to 4; hold all four sessions, then a query's acquire
    // must time out as GraphError::ConnectionAcquireTimeout — not a
    // generic Backend error after sqlx's unrelated 30s default, and not
    // the statement Timeout either (the query never started, so
    // "narrow the traversal" would mislead).
    // A dedicated 1-connection client (the registered source's handle
    // hides the concrete type behind dyn GraphClient).
    let (clean_url, user, pass) = split_creds(&url);
    unsafe {
        if let Some(u) = &user {
            std::env::set_var("SKARDI_AGE_R4_USER", u);
        }
        if let Some(p) = &pass {
            std::env::set_var("SKARDI_AGE_R4_PASS", p);
        }
    }
    let client = AgeClient::connect(
        "saturated",
        &clean_url,
        &graph,
        user.as_ref().map(|_| "SKARDI_AGE_R4_USER"),
        pass.as_ref().map(|_| "SKARDI_AGE_R4_PASS"),
        1, // one connection: held below, so acquire must queue
        std::time::Duration::from_secs(1),
    )
    .await
    .expect("connects");
    let _held = client
        .pool_for_tests()
        .acquire()
        .await
        .expect("hold the only connection");
    let err = client
        .execute(
            "MATCH (p:Person) RETURN p.name",
            &serde_json::json!({}),
            1,
            QueryBounds {
                timeout: std::time::Duration::from_secs(1),
                max_rows: 10,
            },
            None,
        )
        .await
        .err()
        .expect("no connection can be acquired");
    let msg = err.to_string();
    assert!(
        msg.contains("could not acquire a connection"),
        "saturation surfaces as the typed ConnectionAcquireTimeout: {msg}"
    );
    assert!(
        msg.contains("within 1s"),
        "bounded by the configured timeout, not sqlx's 30s default: {msg}"
    );
    drop(_held);

    // ── #2 (demonstration): declared-column order IS the binding — two
    // same-typed columns declared out of RETURN order swap silently.
    // This pins that the documented hazard is real, so the docs can
    // never drift into describing a check that does not exist.
    let batches = collect(
        &ctx,
        "SELECT * FROM cypher_query('kg', \
         'MATCH (p:Person {name: \"ada\"}) RETURN p.name, p.age', '{}', \
         '{\"age\": \"json\", \"name\": \"json\"}')",
    )
    .await;
    let mis_age = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(
        mis_age.value(0),
        "\"ada\"",
        "out-of-order declaration binds positionally: the column NAMED age \
         carries the name value — the silent swap the docs warn about"
    );

    drop_graph(&pool, &graph).await;
}

/// The federation-pushdown contract that replaced the old `register_all`
/// pin (#4 in the live test above): with ONLY the getter UDFs registered —
/// the session shape every front-end now uses — `->>` must NOT be silently
/// rewritten to `json_get`. DataFusion 52 has no native Arrow-operator
/// planner either, so the observable contract is a loud planning error
/// naming the operator. Deliberate, not a regression: see
/// `util::json_getters`' module doc. No backend needed — planning only.
#[tokio::test]
async fn arrow_operators_keep_native_planning() {
    let ctx = SessionContext::new();
    register_json_getter_udfs(&ctx).expect("json getters register");
    let err = ctx
        .sql("SELECT '{\"a\":1}'::text ->> 'a'")
        .await
        .expect_err("no rewrite means no plan");
    let msg = err.to_string();
    assert!(msg.contains("->>"), "the operator is named: {msg}");
    assert!(
        msg.contains("not yet supported"),
        "native (unsupported), not rewritten: {msg}"
    );
}

#[tokio::test]
#[ignore = "needs a live Postgres+AGE (set SKARDI_AGE_LIVE_URL); see module doc"]
async fn a_least_privilege_reader_role_registers_and_queries() {
    // The design's least-privilege recommendation, executed: a plain
    // reader role (no superuser) must register and query. This is what
    // pins `LOAD 'age'` as BEST-EFFORT — a required LOAD is superuser-
    // only for $libdir libraries and would fail this registration on
    // the stock apache/age image (which preloads AGE instead).
    let Some(url) = live_url() else {
        eprintln!("skipping live AGE test: set SKARDI_AGE_LIVE_URL to run");
        return;
    };
    let graph = unique_graph("reader");
    let pool = seed_graph(&url, &graph).await;
    let role = format!("skardi_reader_{graph}");
    for grant in [
        format!("CREATE ROLE {role} LOGIN PASSWORD 'readerpass'"),
        format!("GRANT USAGE ON SCHEMA ag_catalog TO {role}"),
        format!("GRANT SELECT ON ALL TABLES IN SCHEMA ag_catalog TO {role}"),
        format!("GRANT USAGE ON SCHEMA {graph} TO {role}"),
        format!("GRANT SELECT ON ALL TABLES IN SCHEMA {graph} TO {role}"),
    ] {
        pool.execute(grant.as_str()).await.expect("grant");
    }

    let sources: GraphSources = Arc::new(RwLock::new(HashMap::new()));
    let (clean_url, _, _) = split_creds(&url);
    let user_env = format!("SKARDI_AGE_READER_USER_{}", graph.to_uppercase());
    let pass_env = format!("SKARDI_AGE_READER_PASS_{}", graph.to_uppercase());
    unsafe {
        std::env::set_var(&user_env, &role);
        std::env::set_var(&pass_env, "readerpass");
    }
    let config: GraphConfig = serde_yaml::from_str(&format!(
        "backend: age\ngraph_name: {graph}\nquery_timeout_seconds: 10\nmax_rows: 100\n\
         username_env: {user_env}\npassword_env: {pass_env}\n"
    ))
    .expect("config parses");
    register_graph_source(&sources, "kg", &clean_url, &config)
        .await
        .expect("a NON-superuser reader registers (LOAD must be best-effort)");
    let ctx = SessionContext::new();
    register_graph_udtfs(&ctx, Arc::clone(&sources)).expect("udtfs register");
    let batches = collect(
        &ctx,
        "SELECT name FROM cypher_query('kg', 'MATCH (p:Person) RETURN p.name', '{}', \
         '{\"name\": \"string\"}') ORDER BY name",
    )
    .await;
    assert_eq!(
        batches.iter().map(|b| b.num_rows()).sum::<usize>(),
        4,
        "the reader role sees the seeded people"
    );

    drop(sources);
    let _ = pool
        .execute(format!("DROP OWNED BY {role}; DROP ROLE {role};").as_str())
        .await;
    drop_graph(&pool, &graph).await;
}

#[tokio::test]
#[ignore = "needs a live Postgres+AGE (set SKARDI_AGE_LIVE_URL); see module doc"]
async fn cancelled_scans_leave_no_prepared_statements() {
    // The OpenTxnGuard is what RESCUES a cancelled scan's connection
    // back into the pool — so it must also DEALLOCATE the call's
    // prepared statement, or every cancellation strands one `skq_p_*`
    // on the session permanently (monotonic; reproduced in review).
    let Some(url) = live_url() else {
        eprintln!("skipping live AGE test: set SKARDI_AGE_LIVE_URL to run");
        return;
    };
    let graph = unique_graph("cancel");
    let pool = seed_graph(&url, &graph).await;
    let (clean_url, user, pass) = split_creds(&url);
    unsafe {
        std::env::set_var("SKARDI_AGE_CANCEL_USER", user.unwrap_or_default());
        std::env::set_var("SKARDI_AGE_CANCEL_PASS", pass.unwrap_or_default());
    }
    // max_connections = 1: every round and the sweep share ONE session.
    let client = AgeClient::connect(
        "kg",
        &clean_url,
        &graph,
        Some("SKARDI_AGE_CANCEL_USER"),
        Some("SKARDI_AGE_CANCEL_PASS"),
        1,
        std::time::Duration::from_secs(30),
    )
    .await
    .expect("connects");
    let bounds = QueryBounds {
        timeout: std::time::Duration::from_secs(30),
        max_rows: 1_000_000,
    };
    // Slow enough to still be mid-flight at 300ms: a wide cartesian
    // whose predicate REJECTS NOTHING (an equality on a missing name
    // would zero out `a` and finish instantly).
    let slow = "MATCH (a),(b),(c),(d),(e),(f),(g),(h),(i),(j) \
                WHERE a.name <> $x RETURN a";
    let params = serde_json::json!({"x": "nobody"});
    for round in 0..3 {
        let fut = client.execute(slow, &params, 1, bounds, None);
        let outcome = tokio::time::timeout(std::time::Duration::from_millis(300), fut).await;
        assert!(
            outcome.is_err(),
            "round {round}: the future must still be mid-flight when dropped"
        );
    }
    // The guard's cleanup is a spawned task; give it a moment.
    tokio::time::sleep(std::time::Duration::from_millis(1500)).await;
    let mut conn = client
        .pool_for_tests()
        .acquire()
        .await
        .expect("the one session");
    let leaked: Vec<String> =
        sqlx::query_scalar("SELECT name FROM pg_prepared_statements WHERE name LIKE 'skq_p_%'")
            .fetch_all(&mut *conn)
            .await
            .expect("sweep");
    assert!(
        leaked.is_empty(),
        "cancelled scans must deallocate their prepared statements, found: {leaked:?}"
    );
    drop(conn);
    drop_graph(&pool, &graph).await;
}
