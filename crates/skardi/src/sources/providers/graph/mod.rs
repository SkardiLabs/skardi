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
//!   `(label, kind)` row per label off `ag_catalog`, names only;
//! - read-only enforced by the BACKEND (`READ ONLY` transactions), with
//!   the keyword guard as fast-path UX;
//! - every query bounded (`query_timeout_seconds`, `max_rows`) with
//!   typed errors, never silent truncation;
//! - the `datafusion-functions-json` getter family registered alongside,
//!   so `properties` JSON columns are queryable without leaving SQL.
//!
//! Neo4j (Bolt, gated on the access-mode spike) and Kuzu are later
//! milestones behind the same [`client::GraphClient`] trait; YAML
//! catalog views and `type: graph` server registration are milestone 4.

pub mod client;
pub mod config;
pub mod error;
pub mod guard;
pub mod udtf;
pub mod value;

use std::sync::Arc;
use std::time::Duration;

use client::{AgeClient, QueryBounds};
use config::GraphConfig;
use error::GraphError;
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
pub async fn register_graph_source(
    sources: &GraphSources,
    name: &str,
    connection_string: &str,
    config: &GraphConfig,
) -> Result<(), GraphError> {
    config.validate(name, connection_string)?;
    let client = AgeClient::connect(
        name,
        connection_string,
        &config.graph_name,
        config.username_env.as_deref(),
        config.password_env.as_deref(),
    )
    .await?;
    let handle = Arc::new(GraphSourceHandle {
        client: Arc::new(client),
        bounds: QueryBounds {
            timeout: Duration::from_secs(config.query_timeout_seconds),
            max_rows: config.max_rows,
        },
    });
    let mut map = sources.write().map_err(|_| GraphError::InvalidConfig {
        name: name.to_string(),
        reason: "graph sources lock poisoned".to_string(),
    })?;
    if map.insert(name.to_string(), handle).is_some() {
        return Err(GraphError::InvalidConfig {
            name: name.to_string(),
            reason: "a graph source with this name is already registered".to_string(),
        });
    }
    Ok(())
}
