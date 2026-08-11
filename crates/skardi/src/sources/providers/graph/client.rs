//! The backend abstraction (design §Backend abstraction) and its
//! milestone-1 implementation: [`AgeClient`] — Apache AGE, openCypher
//! inside Postgres.
//!
//! One deliberate deviation from the design text, recorded here: the
//! design named `tokio-postgres`; this implementation rides **sqlx**,
//! because the workspace already ships sqlx-postgres for the relational
//! Postgres provider — one Postgres stack beats two (same TLS, same
//! pooling, same error surface).
//!
//! Read-only is backend-enforced: every call runs inside a Postgres
//! `READ ONLY` transaction — the keyword guard upstream is UX, this is
//! the boundary. Every call is bounded: `statement_timeout` server-side
//! plus a client-side wrap, and a row cap enforced while streaming, so a
//! whole-graph `RETURN` fails loudly at `max_rows + 1` instead of
//! buffering the world.

use std::time::Duration;

use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use futures::StreamExt;
use futures::stream::BoxStream;
use serde_json::Value;
use sqlx::postgres::{PgConnectOptions, PgPool, PgPoolOptions};
use sqlx::{Either, Executor, Row};

use super::error::{GraphError, json_kind};
use super::value::parse_agtype;

/// Per-query operational bounds (design §Security and operational
/// bounds), carried per source.
#[derive(Debug, Clone, Copy)]
pub struct QueryBounds {
    pub timeout: Duration,
    pub max_rows: usize,
}

/// One result row: one JSON value per declared column.
pub type GraphRow = Vec<Value>;
/// The stream `execute` returns. Milestone 1 implementations may buffer
/// internally up to `max_rows` — the trait shape is what must not change
/// (design §Backend abstraction).
pub type GraphRowStream = BoxStream<'static, Result<GraphRow, GraphError>>;

/// What a backend must provide. Hides AGE (Postgres wire) vs Neo4j Bolt
/// vs Kuzu details behind one seam.
#[async_trait]
pub trait GraphClient: Send + Sync + std::fmt::Debug {
    /// Run read-only Cypher inside a backend-enforced read transaction,
    /// bounded by `bounds`. `arity` is the declared column count — AGE's
    /// `cypher()` call must declare its result arity, which is why the
    /// declared-columns mode is required on this backend.
    async fn execute(
        &self,
        cypher: &str,
        params: &Value,
        arity: usize,
        bounds: QueryBounds,
    ) -> Result<GraphRowStream, GraphError>;

    /// Label roster for `graph_schema`: one `(label, kind)` row per
    /// label, kinds `vertex` / `edge`. Names only — never property
    /// values (design §Agent and LLM interaction).
    async fn labels(&self) -> Result<Vec<(String, String)>, GraphError>;
}

/// Apache AGE over the workspace's sqlx-postgres stack.
#[derive(Debug)]
pub struct AgeClient {
    pool: PgPool,
    graph_name: String,
    source_name: String,
    /// Uniquifies per-call PREPARE names (see [`Self::build_sql`]) so
    /// pooled-connection reuse can never collide.
    prepare_seq: AtomicU64,
}

impl AgeClient {
    /// Connect a pool whose every connection has AGE loaded and
    /// `ag_catalog` on the search path. Credentials arrive as env-var
    /// NAMES (values never in YAML); when set they override whatever the
    /// URL carries.
    pub async fn connect(
        source_name: &str,
        connection_string: &str,
        graph_name: &str,
        username_env: Option<&str>,
        password_env: Option<&str>,
    ) -> Result<Self, GraphError> {
        let mut options: PgConnectOptions =
            connection_string
                .parse()
                .map_err(|e: sqlx::Error| GraphError::InvalidConfig {
                    name: source_name.to_string(),
                    reason: format!("connection_string does not parse: {e}"),
                })?;
        if let Some(env) = username_env {
            let user = read_env(source_name, "username_env", env)?;
            options = options.username(&user);
        }
        if let Some(env) = password_env {
            let pass = read_env(source_name, "password_env", env)?;
            options = options.password(&pass);
        }
        let pool = PgPoolOptions::new()
            .max_connections(4)
            .after_connect(|conn, _meta| {
                Box::pin(async move {
                    // Per-connection, not per-query: LOAD is connection
                    // state, and ag_catalog on the search path is what
                    // makes `agtype` and `cypher()` resolvable.
                    conn.execute("LOAD 'age'; SET search_path = ag_catalog, \"$user\", public;")
                        .await?;
                    Ok(())
                })
            })
            .connect_with(options)
            .await
            .map_err(|e| backend_error(source_name, &e))?;
        Ok(Self {
            pool,
            graph_name: graph_name.to_string(),
            source_name: source_name.to_string(),
            prepare_seq: AtomicU64::new(0),
        })
    }

    /// The `cypher()` invocation, spoken over the SIMPLE query protocol —
    /// three AGE realities verified live make this the one sound
    /// spelling:
    ///
    /// - `cypher()`'s params argument must be a prepared-statement
    ///   parameter (AGE rejects a constant with "third argument … must be
    ///   a parameter"), so a parameterized call rides the documented
    ///   `PREPARE name(agtype) AS …; EXECUTE name('…');` pattern — with a
    ///   per-call unique name so pooled connections never collide.
    /// - `::text` on agtype is AGE's scalar-only cast (a vertex fails
    ///   with "unsupported argument agtype"), and for scalars it strips
    ///   JSON quoting. The simple protocol instead delivers every column
    ///   through `agtype_out` — the uniform annotated-JSON text the
    ///   decoder expects.
    /// - Everything is constant SQL text: the graph name is
    ///   identifier-validated at config load AND quote-escaped (belt and
    ///   braces); the caller's Cypher rides a dollar-quoted string whose
    ///   tag provably does not occur in it; params are serde_json-encoded
    ///   and single-quote-escaped — values cannot break out of the
    ///   serialization.
    ///
    /// Returns the statement batch and the prepared name to DEALLOCATE
    /// (when params were bound).
    fn build_sql(&self, cypher: &str, params: &Value, arity: usize) -> (String, Option<String>) {
        let tag = dollar_tag(cypher);
        let graph = self.graph_name.replace('\'', "''");
        let cols: Vec<String> = (0..arity)
            .map(|i| format!("c{i} ag_catalog.agtype"))
            .collect();
        let outs: Vec<String> = (0..arity).map(|i| format!("c{i}")).collect();
        let has_params = params.as_object().is_some_and(|m| !m.is_empty());
        if has_params {
            let name = format!(
                "skq_p_{}_{}",
                std::process::id(),
                self.prepare_seq.fetch_add(1, Ordering::Relaxed)
            );
            let literal = params.to_string().replace('\'', "''");
            let batch = format!(
                "PREPARE {name}(ag_catalog.agtype) AS \
                 SELECT {} FROM ag_catalog.cypher('{graph}', {tag}{cypher}{tag}, $1) \
                 AS t({}); \
                 EXECUTE {name}('{literal}');",
                outs.join(", "),
                cols.join(", ")
            );
            (batch, Some(name))
        } else {
            (
                format!(
                    "SELECT {} FROM ag_catalog.cypher('{graph}', {tag}{cypher}{tag}) \
                     AS t({})",
                    outs.join(", "),
                    cols.join(", ")
                ),
                None,
            )
        }
    }
}

#[async_trait]
impl GraphClient for AgeClient {
    async fn execute(
        &self,
        cypher: &str,
        params: &Value,
        arity: usize,
        bounds: QueryBounds,
    ) -> Result<GraphRowStream, GraphError> {
        let (sql, prepared) = self.build_sql(cypher, params, arity);
        let source = self.source_name.clone();
        let run = async {
            let mut tx = self
                .pool
                .begin()
                .await
                .map_err(|e| backend_error(&source, &e))?;
            // The security boundary: the SERVER refuses writes in a read
            // transaction, whatever slipped past the keyword guard.
            sqlx::query("SET TRANSACTION READ ONLY")
                .execute(&mut *tx)
                .await
                .map_err(|e| backend_error(&source, &e))?;
            // Server-side: runaway traversals die in the backend, not in
            // a client that gave up waiting.
            sqlx::query(&format!(
                "SET LOCAL statement_timeout = '{}ms'",
                bounds.timeout.as_millis()
            ))
            .execute(&mut *tx)
            .await
            .map_err(|e| backend_error(&source, &e))?;

            // Stream (simple protocol — see build_sql) and cap:
            // max_rows + 1 proves the overflow without buffering past it.
            let mut rows: Vec<GraphRow> = Vec::new();
            {
                let mut stream = tx.fetch_many(sqlx::raw_sql(&sql));
                while let Some(step) = stream.next().await {
                    let step = step.map_err(|e| map_query_error(&source, bounds, &e))?;
                    // PREPARE contributes a rowless result; only rows count.
                    let Either::Right(row) = step else { continue };
                    if rows.len() >= bounds.max_rows {
                        return Err(GraphError::RowCapExceeded {
                            max_rows: bounds.max_rows,
                        });
                    }
                    let row_idx = rows.len();
                    let mut values = Vec::with_capacity(arity);
                    for col in 0..arity {
                        // Unchecked: simple-protocol cells are agtype_out
                        // text, whose OID sqlx cannot map to String.
                        let text: Option<String> = row
                            .try_get_unchecked(col)
                            .map_err(|e| backend_error(&source, &e))?;
                        let value = match text {
                            None => Value::Null,
                            Some(t) => {
                                parse_agtype(&t).map_err(|reason| GraphError::MalformedCell {
                                    row: row_idx,
                                    column: col,
                                    reason,
                                })?
                            }
                        };
                        values.push(value);
                    }
                    rows.push(values);
                }
            }
            if let Some(name) = &prepared {
                // Targeted DEALLOCATE (never ALL — sqlx's own statement
                // cache lives on the same connection).
                tx.execute(format!("DEALLOCATE {name}").as_str())
                    .await
                    .map_err(|e| backend_error(&source, &e))?;
            }
            // Read-only: rollback returns the connection with nothing to
            // undo either way.
            tx.rollback()
                .await
                .map_err(|e| backend_error(&source, &e))?;
            Ok(rows)
        };
        // Client-side wrap: the server timeout is authoritative, this one
        // covers a backend that stops answering entirely.
        let rows = tokio::time::timeout(bounds.timeout + Duration::from_secs(5), run)
            .await
            .map_err(|_| GraphError::Timeout {
                seconds: bounds.timeout.as_secs(),
            })??;
        Ok(futures::stream::iter(rows.into_iter().map(Ok)).boxed())
    }

    async fn labels(&self) -> Result<Vec<(String, String)>, GraphError> {
        // ag_catalog is the exact source for labels; AGE's own catch-all
        // labels (`_ag_label_vertex` / `_ag_label_edge`) are implementation
        // noise for an agent and are filtered.
        let rows = sqlx::query(
            "SELECT l.name, l.kind::text \
             FROM ag_catalog.ag_label l \
             JOIN ag_catalog.ag_graph g ON g.graphid = l.graph \
             WHERE g.name = $1 AND l.name NOT LIKE '\\_ag\\_label\\_%' \
             ORDER BY l.name",
        )
        .bind(&self.graph_name)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| backend_error(&self.source_name, &e))?;
        rows.into_iter()
            .map(|row| {
                let name: String = row
                    .try_get(0)
                    .map_err(|e| backend_error(&self.source_name, &e))?;
                let kind: String = row
                    .try_get(1)
                    .map_err(|e| backend_error(&self.source_name, &e))?;
                let kind = match kind.as_str() {
                    "v" => "vertex".to_string(),
                    "e" => "edge".to_string(),
                    other => other.to_string(),
                };
                Ok((name, kind))
            })
            .collect()
    }
}

/// A dollar-quote tag that provably does not occur in `text`: extend a
/// base tag with a counter until absent — no escape rules to get wrong.
fn dollar_tag(text: &str) -> String {
    let mut tag = "$skq$".to_string();
    let mut n = 0u32;
    while text.contains(&tag) {
        n += 1;
        tag = format!("$skq{n}$");
    }
    tag
}

fn read_env(source: &str, field: &str, env: &str) -> Result<String, GraphError> {
    std::env::var(env).map_err(|_| GraphError::InvalidConfig {
        name: source.to_string(),
        reason: format!("{field}: ${env} is not set in this environment"),
    })
}

fn backend_error(source: &str, e: &sqlx::Error) -> GraphError {
    let code = match e {
        sqlx::Error::Database(db) => db.code().map(|c| c.to_string()),
        _ => None,
    };
    GraphError::backend(source, code.as_deref().unwrap_or("io"), &e.to_string())
}

/// Postgres cancels a statement-timeout overrun with SQLSTATE 57014 —
/// surface it as the typed timeout, not a generic backend error.
fn map_query_error(source: &str, bounds: QueryBounds, e: &sqlx::Error) -> GraphError {
    if let sqlx::Error::Database(db) = e
        && db.code().as_deref() == Some("57014")
    {
        return GraphError::Timeout {
            seconds: bounds.timeout.as_secs(),
        };
    }
    backend_error(source, e)
}

/// Numeric `params` mapping (design §SQL surface): a JSON number with no
/// fraction or exponent binds as Integer; with either, as Float — write
/// `1.0` to force Float. serde_json preserves the distinction (`is_i64`
/// vs `is_f64`), so validation is a kind check, not a coercion.
pub fn validate_params(params: &Value) -> Result<(), GraphError> {
    match params {
        Value::Object(_) => Ok(()),
        other => Err(GraphError::InvalidParams {
            found: json_kind(other).to_string(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dollar_tags_never_collide_with_the_text() {
        assert_eq!(dollar_tag("MATCH (n) RETURN n"), "$skq$");
        let hostile = "RETURN '$skq$ $skq1$'";
        let tag = dollar_tag(hostile);
        assert!(!hostile.contains(&tag), "{tag}");
    }

    #[test]
    fn params_must_be_an_object() {
        assert!(validate_params(&serde_json::json!({"a": 1})).is_ok());
        let err = validate_params(&serde_json::json!([1])).unwrap_err();
        assert!(err.to_string().contains("an array"), "{err}");
    }
}
