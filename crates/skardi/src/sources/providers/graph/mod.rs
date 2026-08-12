//! Graph engine bypass — read-only Cypher as SQL tables (design:
//! `docs/superpowers/specs/2026-08-08-graph-engine-bypass-design.md`).
//!
//! Skardi does not parse, plan, or store graphs: the graph engine owns
//! storage, indexing, and traversal, and this module forwards read-only
//! Cypher and maps the results into Arrow rows with a
//! **planning-time-stable schema** (caller-declared columns; no probes,
//! no network I/O at planning). Milestone 1 ships the Apache AGE backend
//! (openCypher inside Postgres — the GraphRAG-in-Postgres deployment,
//! zero new infrastructure); milestone 2 adds Neo4j over Bolt behind the
//! same [`client::GraphClient`] trait:
//!
//! - `cypher_query(connection, cypher, params, columns)` — declared
//!   columns required on AGE (its `cypher()` call must declare arity;
//!   binding is positional there) and optional on Neo4j (Bolt carries
//!   field names: binding is BY NAME, and omission is the JSON-`record`
//!   fallback);
//! - `graph_schema(connection)` — the agent-discovery surface, one
//!   `(label, kind, property, property_type)` row per label off the
//!   backend catalog. Names and types only, per what each catalog knows:
//!   AGE serves label names and kinds (schema-optional store, property
//!   columns always null); Neo4j adds property names/types via
//!   `db.schema.nodeTypeProperties()` / `relTypeProperties()`;
//! - read-only enforced by the BACKEND (AGE: `READ ONLY` transactions;
//!   Neo4j: auto-commit READ-access-mode transactions, proven at
//!   registration — see `neo4j.rs`), with the keyword guard as
//!   fast-path UX;
//! - every query bounded (`query_timeout_seconds`, `max_rows`) with
//!   typed errors, never silent truncation;
//! - the `datafusion-functions-json` getter family registered alongside,
//!   so `properties` JSON columns are queryable without leaving SQL.
//!
//! Kuzu is a later milestone behind the same trait; YAML catalog views
//! and `type: graph` server registration are milestone 4.

pub mod client;
pub mod config;
pub mod error;
pub mod guard;
pub mod neo4j;
pub mod udtf;
pub mod value;

use std::sync::Arc;
use std::time::Duration;

use client::{AgeClient, QueryBounds};
use config::GraphConfig;
use error::GraphError;
use neo4j::Neo4jClient;
use udtf::{GraphSourceHandle, GraphSources};

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
    let timeout = Duration::from_secs(config.query_timeout_seconds);
    // Explicit per-backend arms, no wildcard: when milestone 3 adds
    // `kuzu` to validate()'s allowlist, forgetting this match must be a
    // loud unreachable here — never a Kuzu URL silently dialed with the
    // Bolt client.
    let client: Arc<dyn client::GraphClient> = match config.backend.as_str() {
        "age" => Arc::new(
            AgeClient::connect(
                name,
                connection_string,
                config
                    .graph_name
                    .as_deref()
                    .expect("validate() requires graph_name on age"),
                config.username_env.as_deref(),
                config.password_env.as_deref(),
                config.max_connections,
                timeout,
            )
            .await?,
        ),
        "neo4j" => Arc::new(
            Neo4jClient::connect(
                name,
                connection_string,
                config.graph_name.as_deref(),
                config.username_env.as_deref(),
                config.password_env.as_deref(),
                config.max_connections,
                timeout,
            )
            .await?,
        ),
        other => unreachable!("validate() rejected backend '{other}' before dispatch"),
    };
    let handle = Arc::new(GraphSourceHandle {
        client,
        bounds: QueryBounds {
            timeout,
            max_rows: config.max_rows,
        },
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
