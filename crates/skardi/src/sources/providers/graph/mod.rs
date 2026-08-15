//! Graph engine bypass — read-only Cypher as SQL tables (design:
//! `docs/superpowers/specs/2026-08-08-graph-engine-bypass-design.md`).
//!
//! Skardi does not parse, plan, or store graphs: the graph engine owns
//! storage, indexing, and traversal, and this module forwards read-only
//! Cypher and maps the results into Arrow rows with a
//! **planning-time-stable schema** (caller-declared columns; no probes,
//! no network I/O at planning). Milestone 1 ships the Apache AGE backend
//! (openCypher inside Postgres — the GraphRAG-in-Postgres deployment,
//! zero new infrastructure) with:
//!
//! - `cypher_query(connection, cypher, params, columns)` — declared
//!   columns required on AGE (its `cypher()` call must declare arity);
//! - `graph_schema(connection)` — the agent-discovery surface, one
//!   `(label, kind)` row per label off `ag_catalog`, names only (all
//!   the AGE catalog knows: it is schema-optional and declares no
//!   properties — property names/types come with the Neo4j/Kuzu
//!   milestones, whose catalogs carry them);
//! - read-only enforced by the BACKEND (`READ ONLY` transactions), with
//!   the keyword guard as fast-path UX;
//! - every query bounded (`query_timeout_seconds`, `max_rows`) with
//!   typed errors, never silent truncation;
//! - milestone 4: YAML catalog views — each `views:` entry is registered
//!   by [`register_graph_tables`] as the catalog table
//!   `<source>.main.<name>`, with live validation at registration and a
//!   degraded registration path (unreachable backend registers, first
//!   scan retries and fails loudly).
//!
//! The `datafusion-functions-json` getter family is NOT registered by
//! this module: its `register_all` rewrites `->`/`->>`/`?` session-wide,
//! so the getters are the server session's own unconditional
//! registration; engine-API users register the individual UDFs.
//!
//! Neo4j (Bolt, gated on the access-mode spike) and Kuzu are later
//! milestones behind the same [`client::GraphClient`] trait.

pub mod client;
pub mod config;
pub mod error;
pub mod guard;
pub mod udtf;
pub mod value;
mod view;

use std::sync::Arc;
use std::sync::RwLock;
use std::time::Duration;

use datafusion::catalog::{
    CatalogProvider, MemoryCatalogProvider, MemorySchemaProvider, SchemaProvider,
};
use datafusion::prelude::SessionContext;

use client::{AgeClient, GraphClient, QueryBounds};
use config::GraphConfig;
use error::GraphError;
use udtf::{GraphSourceHandle, GraphSourceHealth, GraphSources};
use view::{GraphViewProvider, validate_view};

use crate::sources::hierarchy::HierarchyLevel;

pub use udtf::register_graph_udtfs;

/// Connect and register one `type: graph` source under `name`, making it
/// resolvable from `cypher_query('<name>', …)` / `graph_schema('<name>')`.
///
/// Config validation is pure and runs first; the connection pool is then
/// established eagerly so a wrong URL or credential fails HERE, at
/// registration, with the source named — not at first query.
///
/// # Errors
/// [`GraphError::InvalidConfig`] for validation failures;
/// [`GraphError::Backend`] when the backend refuses the connection.
///
/// # Example
/// ```no_run
/// use std::collections::HashMap;
/// use std::sync::{Arc, RwLock};
/// use skardi::sources::providers::graph::config::GraphConfig;
/// use skardi::sources::providers::graph::udtf::GraphSources;
/// use skardi::sources::providers::graph::register_graph_source;
///
/// # async fn demo() -> Result<(), Box<dyn std::error::Error>> {
/// let sources: GraphSources = Arc::new(RwLock::new(HashMap::new()));
/// let config: GraphConfig =
///     serde_yaml::from_str("backend: age\ngraph_name: knowledge\n")?;
/// register_graph_source(
///     &sources,
///     "kg",
///     "postgres://localhost:5432/graphrag",
///     &config,
/// )
/// .await?;
/// // cypher_query('kg', …) and graph_schema('kg') now resolve.
/// # Ok(())
/// # }
/// ```
pub async fn register_graph_source(
    sources: &GraphSources,
    name: &str,
    connection_string: &str,
    config: &GraphConfig,
) -> Result<(), GraphError> {
    config.validate(name, connection_string)?;
    // Check-early AND check-again: this peek spares a doomed registration
    // the full eager connect (pool + graph probe, possibly a slow
    // backend); the entry-based check after connect is what actually
    // closes the race between two concurrent registrations.
    {
        let map = sources.read().unwrap_or_else(|p| p.into_inner());
        if map.contains_key(name) {
            return Err(GraphError::InvalidConfig {
                name: name.to_string(),
                reason: "a graph source with this name is already registered \
                         (the existing connection is unchanged)"
                    .to_string(),
            });
        }
    }
    let client = AgeClient::connect(
        name,
        connection_string,
        &config.graph_name,
        config.username_env.as_deref(),
        config.password_env.as_deref(),
        config.max_connections,
        Duration::from_secs(config.query_timeout_seconds),
    )
    .await?;
    let handle = Arc::new(GraphSourceHandle {
        client: Arc::new(client),
        bounds: QueryBounds {
            timeout: Duration::from_secs(config.query_timeout_seconds),
            max_rows: config.max_rows,
        },
        // connect() preflighted successfully, or we would not be here.
        health: Arc::new(RwLock::new(GraphSourceHealth::Healthy)),
        // The engine-level entry registers no views (no session context
        // to put them in), so there are no contracts to re-prove.
        view_contracts: Arc::new(vec![]),
    });
    // Poisoning degrades gracefully (AGENTS.md convention) — and it also
    // keeps InvalidConfig meaning what it says instead of moonlighting as
    // a lock error. Entry-based: a duplicate name must leave the ORIGINAL
    // handle untouched — insert-then-check would replace the live
    // connection and then report failure, leaving queries silently
    // routed to the new one.
    let mut map = sources.write().unwrap_or_else(|p| p.into_inner());
    match map.entry(name.to_string()) {
        std::collections::hash_map::Entry::Occupied(_) => Err(GraphError::InvalidConfig {
            name: name.to_string(),
            reason: "a graph source with this name is already registered \
                     (the existing connection is unchanged)"
                .to_string(),
        }),
        std::collections::hash_map::Entry::Vacant(slot) => {
            slot.insert(handle);
            Ok(())
        }
    }
}

/// Register a `type: graph` data source into a DataFusion
/// [`SessionContext`] (the server entry point, milestone 4): the UDTF
/// handle plus one catalog table `<name>.main.<view>` per declared view.
///
/// The order is load-bearing:
///
/// 1. **Invariants first, before any network I/O** — this is the single
///    enforcement point both front-ends share: read-only milestone,
///    [`HierarchyLevel::Catalog`], a present `graph:` block, and
///    [`GraphConfig::validate`].
/// 2. **Availability and contract violations part ways** (design §Schema
///    handling): an UNREACHABLE backend registers the source DEGRADED —
///    a shared external database's transient blip must not hold every
///    unrelated source hostage at server startup — and the first view
///    scan retries the validation, failing loudly if the backend is
///    still gone. A REACHABLE backend whose view fails validation
///    REFUSES registration: that is a contract violation, not an outage.
///
/// # Errors
/// [`GraphError::InvalidConfig`] for invariant/validation failures and
/// reachable-but-broken views; registration itself never fails on an
/// unreachable backend.
///
/// # Example
/// ```no_run
/// use std::collections::HashMap;
/// use std::sync::{Arc, RwLock};
/// use datafusion::prelude::SessionContext;
/// use skardi::sources::hierarchy::HierarchyLevel;
/// use skardi::sources::providers::graph::config::GraphConfig;
/// use skardi::sources::providers::graph::udtf::GraphSources;
/// use skardi::sources::providers::graph::register_graph_tables;
///
/// # async fn demo() -> Result<(), Box<dyn std::error::Error>> {
/// let sources: GraphSources = Arc::new(RwLock::new(HashMap::new()));
/// let config: GraphConfig = serde_yaml::from_str(
///     "backend: age\ngraph_name: knowledge\nviews:\n  \
///      - name: user_posts\n    \
///      cypher: MATCH (u:User)-[:POSTED]->(p:Post) RETURN u.name AS user_name\n    \
///      schema:\n      - name: user_name\n        type: string\n",
/// )?;
/// let mut ctx = SessionContext::new();
/// register_graph_tables(
///     &mut ctx,
///     &sources,
///     "kg",
///     "postgres://localhost:5432/graphrag",
///     Some(&config),
///     false,
///     HierarchyLevel::Catalog,
/// )
/// .await?;
/// // SELECT * FROM kg.main.user_posts now plans against the declared schema.
/// # Ok(())
/// # }
/// ```
pub async fn register_graph_tables(
    session_ctx: &mut SessionContext,
    sources: &GraphSources,
    name: &str,
    connection_string: &str,
    config: Option<&GraphConfig>,
    read_write: bool,
    hierarchy_level: HierarchyLevel,
) -> Result<(), GraphError> {
    // Invariants before any network I/O — a doomed registration must not
    // pay for a connection attempt (or hang on an unreachable host).
    if read_write {
        return Err(GraphError::InvalidConfig {
            name: name.to_string(),
            reason: "read-only milestone; writes are rejected by the backend's \
                     READ ONLY transaction"
                .to_string(),
        });
    }
    if hierarchy_level != HierarchyLevel::Catalog {
        return Err(GraphError::InvalidConfig {
            name: name.to_string(),
            reason: "type: graph requires hierarchy: catalog (views register as \
                     <name>.main.<view> catalog tables)"
                .to_string(),
        });
    }
    let Some(config) = config else {
        return Err(GraphError::InvalidConfig {
            name: name.to_string(),
            reason: "type: graph requires a graph: block".to_string(),
        });
    };
    config.validate(name, connection_string)?;
    // Peek first, entry-check last — the same double-check discipline as
    // register_graph_source (the peek spares a doomed registration the
    // connect; the entry check closes the race).
    {
        let map = sources.read().unwrap_or_else(|p| p.into_inner());
        if map.contains_key(name) {
            return Err(GraphError::InvalidConfig {
                name: name.to_string(),
                reason: "a graph source with this name is already registered \
                         (the existing connection is unchanged)"
                    .to_string(),
            });
        }
    }
    let timeout = Duration::from_secs(config.query_timeout_seconds);
    let bounds = QueryBounds {
        timeout,
        max_rows: config.max_rows,
    };
    let (client, health): (Arc<dyn GraphClient>, GraphSourceHealth) = match AgeClient::connect(
        name,
        connection_string,
        &config.graph_name,
        config.username_env.as_deref(),
        config.password_env.as_deref(),
        config.max_connections,
        timeout,
    )
    .await
    {
        Ok(client) => {
            // Reachable backend: every view must prove itself NOW.
            // Any failure is a contract violation, not an outage —
            // refuse registration (nothing has been published yet).
            // Validations are independent read-only probes fetching at
            // most one row each — concurrent, so N views cost one
            // round-trip's latency, not N serial ones at startup.
            let client: Arc<dyn GraphClient> = Arc::new(client);
            let probe = Arc::new(GraphSourceHandle {
                client: Arc::clone(&client),
                bounds,
                health: Arc::new(RwLock::new(GraphSourceHealth::Healthy)),
                view_contracts: Arc::new(vec![]),
            });
            futures::future::try_join_all(config.views.iter().map(|view| {
                let probe = Arc::clone(&probe);
                async move {
                    let columns = view.declared_columns()?;
                    validate_view(&probe, &view.name, &view.cypher, &columns).await
                }
            }))
            .await?;
            (client, GraphSourceHealth::Healthy)
        }
        Err(e) => {
            // Only a genuine AVAILABILITY failure may degrade: DNS, a
            // refused dial, a network timeout — no server answered.
            // Everything else (bad credentials, AGE absent, the graph
            // missing) is a configuration problem the server ANSWERED;
            // degrading those would let a typo'd graph_name sail through
            // startup and sit degraded forever, the exact failure the
            // eager preflight exists to prevent.
            if !matches!(e, GraphError::Unavailable { .. }) {
                return Err(e);
            }
            // Unreachable backend: register DEGRADED. The design's
            // divergence from Open Connector's hard-fail health check —
            // this backend is a shared external database whose transient
            // blip must not hold every unrelated source hostage at
            // startup. connect_degraded still hard-fails config errors
            // (URL parse, unset credential env); only the dial is lazy.
            tracing::warn!(
                source = name,
                error = %e,
                "graph backend unreachable at registration; registering degraded \
                 (first scan retries the validation)"
            );
            let client = AgeClient::connect_degraded(
                name,
                connection_string,
                &config.graph_name,
                config.username_env.as_deref(),
                config.password_env.as_deref(),
                config.max_connections,
                timeout,
            )?;
            (
                Arc::new(client) as Arc<dyn GraphClient>,
                GraphSourceHealth::Degraded(e.to_string()),
            )
        }
    };
    let handle = Arc::new(GraphSourceHandle {
        client,
        bounds,
        health: Arc::new(RwLock::new(health)),
        // The recovery contract: a degraded source flips Healthy only
        // after EVERY view re-validates — the same all-or-nothing line
        // reachable registration holds.
        view_contracts: Arc::new(
            config
                .views
                .iter()
                .map(|v| {
                    Ok(view::ViewContract {
                        name: v.name.clone(),
                        cypher: v.cypher.clone(),
                        columns: v.declared_columns()?,
                    })
                })
                .collect::<Result<Vec<_>, GraphError>>()?,
        ),
    });
    // Publish the handle BEFORE touching the catalog: register_catalog
    // REPLACES a same-named catalog unconditionally, so with the reverse
    // order a duplicate name would leave the catalog pointing at the new
    // handle while the UDTF map keeps the old one — a split state. The
    // entry check here is what makes the duplicate fail before any
    // catalog is replaced (the peek above merely spared the connect).
    {
        let mut map = sources.write().unwrap_or_else(|p| p.into_inner());
        match map.entry(name.to_string()) {
            std::collections::hash_map::Entry::Occupied(_) => {
                return Err(GraphError::InvalidConfig {
                    name: name.to_string(),
                    reason: "a graph source with this name is already registered \
                             (the existing connection is unchanged)"
                        .to_string(),
                });
            }
            std::collections::hash_map::Entry::Vacant(slot) => {
                slot.insert(Arc::clone(&handle));
            }
        }
    }
    if !config.views.is_empty() {
        let catalog = Arc::new(MemoryCatalogProvider::new());
        let schema_provider = Arc::new(MemorySchemaProvider::new());
        let build = (|| {
            for view in &config.views {
                let provider = GraphViewProvider::new(
                    Arc::clone(&handle),
                    view.name.clone(),
                    view.cypher.clone(),
                    view.declared_columns()?,
                );
                schema_provider
                    .register_table(view.name.clone(), Arc::new(provider))
                    .map_err(|e| GraphError::InvalidConfig {
                        name: name.to_string(),
                        reason: format!("failed to register view '{}': {e}", view.name),
                    })?;
            }
            catalog
                .register_schema("main", schema_provider)
                .map_err(|e| GraphError::InvalidConfig {
                    name: name.to_string(),
                    reason: format!("failed to register the 'main' schema: {e}"),
                })?;
            Ok::<_, GraphError>(catalog)
        })();
        match build {
            Ok(catalog) => {
                session_ctx.register_catalog(name, catalog);
                Ok(())
            }
            Err(e) => {
                // The handle went in first, so a catalog-build failure
                // must take it back out — no handle-without-catalog
                // residue either.
                sources
                    .write()
                    .unwrap_or_else(|p| p.into_inner())
                    .remove(name);
                Err(e)
            }
        }
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    use async_trait::async_trait;
    use futures::StreamExt;
    use futures::stream::{self, BoxStream};
    use serde_json::Value;

    use client::GraphRow;

    /// A never-called client, for pre-network invariant tests.
    #[derive(Debug)]
    struct StubClient;

    #[async_trait]
    impl GraphClient for StubClient {
        async fn execute(
            &self,
            _cypher: &str,
            _params: &Value,
            _arity: usize,
            _bounds: QueryBounds,
            _limit: Option<usize>,
        ) -> Result<BoxStream<'static, Result<GraphRow, GraphError>>, GraphError> {
            Ok(stream::iter(vec![]).boxed())
        }

        async fn labels(
            &self,
            _bounds: QueryBounds,
            _limit: Option<usize>,
        ) -> Result<Vec<(String, String)>, GraphError> {
            Ok(vec![])
        }
    }

    fn sources() -> GraphSources {
        Arc::new(RwLock::new(HashMap::new()))
    }

    fn config_with_views() -> GraphConfig {
        serde_yaml::from_str(
            r#"
backend: age
graph_name: knowledge
query_timeout_seconds: 1
views:
  - name: user_posts
    cypher: MATCH (u:User) RETURN u.name AS user_name
    schema:
      - name: user_name
        type: string
"#,
        )
        .expect("parses")
    }

    /// Deliberately unroutable: connection refused is instant, so the
    /// connect attempt fails fast — and invariant checks must fire
    /// BEFORE it is even attempted.
    const DEAD_URL: &str = "postgres://127.0.0.1:1/none";

    #[tokio::test]
    async fn invariants_are_rejected_before_any_network() {
        let config = config_with_views();
        // read_write.
        let mut ctx = SessionContext::new();
        let err = register_graph_tables(
            &mut ctx,
            &sources(),
            "kg",
            DEAD_URL,
            Some(&config),
            true,
            HierarchyLevel::Catalog,
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("read-only"), "{err}");
        // Non-catalog hierarchy.
        let mut ctx = SessionContext::new();
        let err = register_graph_tables(
            &mut ctx,
            &sources(),
            "kg",
            DEAD_URL,
            Some(&config),
            false,
            HierarchyLevel::Table,
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("hierarchy: catalog"), "{err}");
        // Missing graph: block.
        let mut ctx = SessionContext::new();
        let err = register_graph_tables(
            &mut ctx,
            &sources(),
            "kg",
            DEAD_URL,
            None,
            false,
            HierarchyLevel::Catalog,
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("graph: block"), "{err}");
        // Failing config validation (bad backend) — named by validate.
        let bad: GraphConfig =
            serde_yaml::from_str("backend: neo4j\ngraph_name: g\n").expect("parses");
        let mut ctx = SessionContext::new();
        let err = register_graph_tables(
            &mut ctx,
            &sources(),
            "kg",
            DEAD_URL,
            Some(&bad),
            false,
            HierarchyLevel::Catalog,
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("not supported"), "{err}");
        // A duplicate name — the existing handle is left untouched.
        let existing = sources();
        existing.write().unwrap_or_else(|p| p.into_inner()).insert(
            "kg".to_string(),
            Arc::new(GraphSourceHandle {
                client: Arc::new(StubClient),
                bounds: QueryBounds {
                    timeout: Duration::from_secs(1),
                    max_rows: 10,
                },
                health: Arc::new(RwLock::new(GraphSourceHealth::Healthy)),
                view_contracts: Arc::new(vec![]),
            }),
        );
        let mut ctx = SessionContext::new();
        let err = register_graph_tables(
            &mut ctx,
            &existing,
            "kg",
            DEAD_URL,
            Some(&config),
            false,
            HierarchyLevel::Catalog,
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("already registered"), "{err}");
    }

    #[tokio::test]
    async fn an_unreachable_backend_registers_degraded_and_the_first_scan_fails_loudly() {
        let sources = sources();
        let mut ctx = SessionContext::new();
        register_graph_tables(
            &mut ctx,
            &sources,
            "kg",
            DEAD_URL,
            Some(&config_with_views()),
            false,
            HierarchyLevel::Catalog,
        )
        .await
        .expect("an unreachable backend still registers (degraded)");
        // The handle is published, marked Degraded with the cause.
        let handle = {
            let map = sources.read().unwrap_or_else(|p| p.into_inner());
            Arc::clone(map.get("kg").expect("registered"))
        };
        {
            let health = handle.health.read().unwrap_or_else(|p| p.into_inner());
            match &*health {
                GraphSourceHealth::Degraded(reason) => {
                    assert!(!reason.is_empty(), "the cause is carried");
                }
                GraphSourceHealth::Healthy => panic!("must register degraded"),
            }
        }
        // The declared schema is queryable at PLAN time — no backend.
        let df = ctx
            .sql("SELECT user_name FROM kg.main.user_posts")
            .await
            .expect("the declared schema plans");
        assert_eq!(df.schema().field(0).name(), "user_name");
        // The first scan retries the validation and fails loudly, naming
        // the view (acquire against the closed port is bounded by the
        // config's 1s timeout).
        let err = df.collect().await.expect_err("the backend is still gone");
        let msg = err.to_string();
        assert!(msg.contains("user_posts"), "the view is named: {msg}");
        assert!(msg.contains("DEGRADED"), "{msg}");
        // …and the source stays Degraded afterwards.
        assert!(
            !handle
                .health
                .read()
                .unwrap_or_else(|p| p.into_inner())
                .is_healthy()
        );
    }

    #[tokio::test]
    async fn a_blackholed_backend_registers_degraded_within_the_bound() {
        // The registration-side pin for the preflight timeout: a
        // blackholed address (unroutable, no RST) must degrade within
        // the configured bound — never hang startup on the OS TCP
        // timeout.
        let config: GraphConfig =
            serde_yaml::from_str("backend: age\ngraph_name: knowledge\nquery_timeout_seconds: 1\n")
                .expect("parses");
        let sources = sources();
        let mut ctx = SessionContext::new();
        let started = std::time::Instant::now();
        register_graph_tables(
            &mut ctx,
            &sources,
            "kg",
            "postgres://10.255.255.1:5432/none",
            Some(&config),
            false,
            HierarchyLevel::Catalog,
        )
        .await
        .expect("a blackholed backend still registers (degraded)");
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "startup is not held hostage"
        );
        let handle = Arc::clone(
            sources
                .read()
                .unwrap_or_else(|p| p.into_inner())
                .get("kg")
                .expect("registered"),
        );
        assert!(
            !handle
                .health
                .read()
                .unwrap_or_else(|p| p.into_inner())
                .is_healthy(),
            "the blackholed source is degraded"
        );
    }

    #[tokio::test]
    async fn a_reserved_catalog_name_is_rejected_before_any_network() {
        // `register_catalog` replaces unconditionally, so a source named
        // `datafusion` would clobber the built-in catalog (and every
        // table in it) — caught in pure validation, before any dial.
        let mut ctx = SessionContext::new();
        let err = register_graph_tables(
            &mut ctx,
            &sources(),
            "datafusion",
            DEAD_URL,
            Some(&config_with_views()),
            false,
            HierarchyLevel::Catalog,
        )
        .await
        .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("reserved"), "{msg}");
        assert!(ctx.catalog("datafusion").is_some(), "built-in untouched");
    }

    #[tokio::test]
    async fn a_duplicate_registration_leaves_catalog_and_handle_unsplit() {
        // First: a full degraded registration — handle AND catalog
        // published.
        let sources = sources();
        let mut ctx = SessionContext::new();
        register_graph_tables(
            &mut ctx,
            &sources,
            "kg",
            DEAD_URL,
            Some(&config_with_views()),
            false,
            HierarchyLevel::Catalog,
        )
        .await
        .expect("degraded registration");
        let original_handle = Arc::clone(
            sources
                .read()
                .unwrap_or_else(|p| p.into_inner())
                .get("kg")
                .expect("registered"),
        );
        let original_catalog = ctx.catalog("kg").expect("catalog registered");

        // Second: a duplicate attempt must fail WITHOUT the catalog
        // having been replaced underneath the surviving handle — the
        // split state the entry-first order exists to prevent.
        let err = register_graph_tables(
            &mut ctx,
            &sources,
            "kg",
            DEAD_URL,
            Some(&config_with_views()),
            false,
            HierarchyLevel::Catalog,
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("already registered"), "{err}");
        let handle = Arc::clone(
            sources
                .read()
                .unwrap_or_else(|p| p.into_inner())
                .get("kg")
                .expect("still registered"),
        );
        assert!(
            Arc::ptr_eq(&original_handle, &handle),
            "the original handle survives"
        );
        let catalog = ctx.catalog("kg").expect("catalog still registered");
        assert!(
            Arc::ptr_eq(&original_catalog, &catalog),
            "the original catalog was never replaced"
        );
    }
}
