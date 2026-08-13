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
use sqlx::{Connection, Either, Executor, Row};

use super::error::{GraphError, json_kind};
use super::value::{DeclaredColumn, parse_agtype};

/// Per-query operational bounds (design §Security and operational
/// bounds), carried per source.
#[derive(Debug, Clone, Copy)]
pub struct QueryBounds {
    pub timeout: Duration,
    pub max_rows: usize,
}

/// One result row: one JSON value per declared column (or a single
/// whole-record JSON object in the fallback mode).
pub type GraphRow = Vec<Value>;
/// The stream `execute` returns. Milestone 1 implementations may buffer
/// internally up to `max_rows` — the trait shape is what must not change
/// (design §Backend abstraction).
pub type GraphRowStream = BoxStream<'static, Result<GraphRow, GraphError>>;

/// One `graph_schema` row: a label or relationship type, and — where the
/// backend's catalog carries them (Neo4j, Kuzu; never AGE, which is
/// schema-optional) — one property name and its type(s) per row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaRow {
    pub label: String,
    /// `vertex` or `edge`.
    pub kind: String,
    /// Property NAME — never a value (design §Agent and LLM interaction).
    pub property: Option<String>,
    /// The backend's type name(s) for the property, `|`-joined when the
    /// catalog reports several.
    pub property_type: Option<String>,
}

/// What a backend must provide. Hides AGE (Postgres wire) vs Neo4j Bolt
/// vs Kuzu details behind one seam.
///
/// # Example
/// ```
/// use async_trait::async_trait;
/// use futures::StreamExt;
/// use serde_json::Value;
/// use skardi::sources::providers::graph::client::{
///     GraphClient, GraphRowStream, QueryBounds, SchemaRow,
/// };
/// use skardi::sources::providers::graph::error::GraphError;
/// use skardi::sources::providers::graph::value::DeclaredColumn;
///
/// /// A canned backend, the shape tests use.
/// #[derive(Debug)]
/// struct Fixed(Vec<Vec<Value>>);
///
/// #[async_trait]
/// impl GraphClient for Fixed {
///     async fn execute(
///         &self,
///         _cypher: &str,
///         _params: &Value,
///         _columns: Option<&[DeclaredColumn]>,
///         _bounds: QueryBounds,
///         limit: Option<usize>,
///     ) -> Result<GraphRowStream, GraphError> {
///         let mut rows = self.0.clone();
///         if let Some(l) = limit {
///             rows.truncate(l);
///         }
///         Ok(futures::stream::iter(rows.into_iter().map(Ok)).boxed())
///     }
///
///     fn declared_columns_requirement(&self) -> Option<&'static str> {
///         None
///     }
///
///     async fn schema(
///         &self,
///         _bounds: QueryBounds,
///         _limit: Option<usize>,
///     ) -> Result<Vec<SchemaRow>, GraphError> {
///         Ok(vec![SchemaRow {
///             label: "Person".into(),
///             kind: "vertex".into(),
///             property: None,
///             property_type: None,
///         }])
///     }
/// }
/// ```
#[async_trait]
pub trait GraphClient: Send + Sync + std::fmt::Debug {
    /// Run read-only Cypher inside a backend-enforced read transaction,
    /// bounded by `bounds`.
    ///
    /// `columns` carries the caller's declared columns — how they BIND is
    /// per backend: AGE binds positionally (its `cypher()` call declares
    /// arity, all it gives us), Neo4j binds BY NAME to the Bolt record's
    /// field names. `None` is the JSON-`record` fallback (whole record as
    /// one JSON object per row) — backends whose wire needs a declared
    /// arity refuse it (see [`Self::declared_columns_requirement`]).
    ///
    /// `limit` is pushed as far down as the backend allows (AGE: a real
    /// SQL LIMIT of min(limit, max_rows + 1); Neo4j: bounded stream
    /// consumption over Bolt's incremental PULL) AND enforced at
    /// consumption as defense in depth. Hitting `limit` is a clean early
    /// stop; the row cap stays the loud overflow signal.
    async fn execute(
        &self,
        cypher: &str,
        params: &Value,
        columns: Option<&[DeclaredColumn]>,
        bounds: QueryBounds,
        limit: Option<usize>,
    ) -> Result<GraphRowStream, GraphError>;

    /// Why `execute` would refuse `columns: None`, if it would: `Some`
    /// carries the backend's own explanation (AGE: its `cypher()` must
    /// declare arity, and binding is positional), which the UDTF renders
    /// into the targeted plan-time error — so shared code never
    /// hardcodes one backend's contract. `None` means the JSON-`record`
    /// fallback is available.
    fn declared_columns_requirement(&self) -> Option<&'static str>;

    /// Catalog roster for `graph_schema`: one row per label (kinds
    /// `vertex` / `edge`), and — where the backend's catalog knows them —
    /// one row per (label, property) with the property's name and
    /// type(s). Names and types only, never property values (design
    /// §Agent and LLM interaction). Bounded like every other query —
    /// "every query is bounded" has no catalog exemption — and `limit`
    /// is honored as a clean early stop, pushed as deep as the backend
    /// allows (AGE: into the catalog SQL; Neo4j: applied after the
    /// bounded procedure fetch, since procedure rows flatten 1-to-many).
    async fn schema(
        &self,
        bounds: QueryBounds,
        limit: Option<usize>,
    ) -> Result<Vec<SchemaRow>, GraphError>;
}

/// Cancellation safety for the hand-rolled transaction: [`AgeClient::execute`]
/// manages BEGIN/ROLLBACK manually (rollback must precede DEALLOCATE on
/// the same session — sqlx's `Transaction` guard cannot order that), and
/// the cost of going manual is that NOTHING rolls back if the scan
/// future is dropped mid-transaction — DataFusion drops scan streams on
/// client disconnect, plan short-circuits, and sibling-partition errors,
/// and sqlx's pool return path never rolls back (it pings). A connection
/// returned `idle in transaction` mostly self-heals on reuse, but it
/// holds a snapshot while idle and a cancellation burst can pin every
/// pooled session. This guard closes it: if dropped while ARMED, the
/// connection is moved into a spawned task that rolls back AND
/// deallocates the call's prepared statement, and only then returns it
/// to the pool; the clean path defuses and runs its ordered cleanup
/// inline. The DEALLOCATE matters as much as the rollback: the guard is
/// what RESCUES the connection back into the pool, so without it every
/// cancelled parameterized query would strand one session-level
/// `skq_p_*` statement permanently — monotonic growth on long-lived
/// sessions, the exact failure the live leak sweep exists to prevent.
struct OpenTxnGuard {
    conn: Option<sqlx::pool::PoolConnection<sqlx::Postgres>>,
    /// The per-call PREPARE name, armed as soon as it is known.
    prepared: Option<String>,
}

impl OpenTxnGuard {
    fn conn(&mut self) -> &mut sqlx::PgConnection {
        self.conn.as_mut().expect("armed until defused")
    }

    fn defuse(mut self) -> sqlx::pool::PoolConnection<sqlx::Postgres> {
        self.conn.take().expect("defused exactly once")
    }
}

impl Drop for OpenTxnGuard {
    fn drop(&mut self) {
        if let Some(mut conn) = self.conn.take() {
            let prepared = self.prepared.take();
            // Drop is sync; the cleanup needs the runtime. Execution
            // always runs inside tokio here — the fallback (no runtime:
            // plain drop, sqlx pings a possibly-in-transaction session
            // back into the pool) is only reachable from exotic test
            // harnesses.
            if let Ok(handle) = tokio::runtime::Handle::try_current() {
                handle.spawn(async move {
                    // ROLLBACK first (clears an aborted transaction, where
                    // DEALLOCATE is refused), then drop the statement —
                    // the same order the clean path uses.
                    let _ = conn.execute("ROLLBACK").await;
                    if let Some(name) = prepared {
                        let _ = conn.execute(format!("DEALLOCATE {name}").as_str()).await;
                    }
                });
            }
        }
    }
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
        max_connections: u32,
        acquire_timeout: Duration,
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
        // PREFLIGHT on a single direct connection, before any pool
        // exists: sqlx treats an `after_connect` error as a failed
        // attempt and RETRIES until acquire_timeout, discarding the
        // underlying cause — a bad search_path or a missing AGE would
        // surface as a 30-second "pool timed out" pointing at pool
        // sizing. Running the same per-connection setup here first means
        // auth failures, setup failures, and the graph probe all fail
        // FAST with their real, named error (the eager-fail contract in
        // mod.rs's docstring).
        {
            let mut conn = sqlx::postgres::PgConnection::connect_with(&options)
                .await
                .map_err(|e| backend_error(source_name, &e))?;
            per_connection_setup(&mut conn)
                .await
                .map_err(|e| backend_error(source_name, &e))?;
            // The graph name gets the same eager treatment as the URL
            // and the credential: a typo would otherwise fail LATE and
            // split — per-query raw backend errors on cypher_query, and
            // zero rows WITHOUT error on graph_schema (the catalog join
            // just misses), which an agent reads as "this graph is
            // empty".
            let exists: Option<i32> =
                sqlx::query_scalar("SELECT 1 FROM ag_catalog.ag_graph WHERE name = $1")
                    .bind(graph_name)
                    .fetch_optional(&mut conn)
                    .await
                    .map_err(|e| backend_error(source_name, &e))?;
            if exists.is_none() {
                return Err(GraphError::InvalidConfig {
                    name: source_name.to_string(),
                    reason: format!(
                        "graph '{graph_name}' does not exist in this database \
                         (ag_catalog.ag_graph has no such entry; create it with \
                         SELECT create_graph(...) or fix graph_name)"
                    ),
                });
            }
        }
        // Lazy pool: the preflight above already proved the environment,
        // so the pool's own connections dial on demand.
        let pool = PgPoolOptions::new()
            .max_connections(max_connections)
            // Queueing on a saturated pool is bounded by the SAME knob
            // as the query itself — sqlx's 30s default is unrelated to
            // query_timeout_seconds and would surface as a generic
            // driver failure after possibly LONGER than the configured
            // bound ("every query is bounded" covers the queue too).
            .acquire_timeout(acquire_timeout)
            .after_connect(|conn, _meta| Box::pin(per_connection_setup(conn)))
            .connect_lazy_with(options);
        tracing::debug!(
            source = source_name,
            graph = graph_name,
            "graph source connected"
        );
        Ok(Self {
            pool,
            graph_name: graph_name.to_string(),
            source_name: source_name.to_string(),
            prepare_seq: AtomicU64::new(0),
        })
    }

    /// The pool, for the live leak-regression test ONLY (the
    /// `pg_prepared_statements` sweep is session-local, so it must run
    /// on the very sessions `execute` used). Not API.
    #[doc(hidden)]
    pub fn pool_for_tests(&self) -> &PgPool {
        &self.pool
    }
}

/// Per-connection session state, shared by the registration preflight
/// and the pool's `after_connect` hook so the two can never drift.
///
/// `LOAD 'age'` is BEST-EFFORT, deliberately: Postgres restricts LOAD
/// of a library outside `$libdir/plugins` to superusers (AGE installs
/// to `$libdir/age.so`), so requiring it would force every graph source
/// onto a superuser credential — the exact opposite of the design's
/// least-privilege recommendation, and it would put the module's ONLY
/// enforcing layer (the backend READ ONLY transaction) behind maximum
/// privilege. The supported deployment (the official apache/age image)
/// ships `shared_preload_libraries = age`, where the LOAD is a no-op;
/// where AGE is genuinely absent, the registration preflight's
/// `ag_catalog.ag_graph` probe is what fails, with a named error. The
/// search_path, by contrast, is required state — its failure is real.
async fn per_connection_setup(conn: &mut sqlx::PgConnection) -> Result<(), sqlx::Error> {
    let _ = conn.execute("LOAD 'age'").await;
    conn.execute("SET search_path = ag_catalog, \"$user\", public")
        .await?;
    Ok(())
}

impl AgeClient {
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
    fn build_sql(
        &self,
        cypher: &str,
        params: &Value,
        arity: usize,
        fetch: usize,
    ) -> (String, Option<String>) {
        build_cypher_sql(
            &self.graph_name,
            self.prepare_seq.fetch_add(1, Ordering::Relaxed),
            cypher,
            params,
            arity,
            fetch,
        )
    }
}

#[async_trait]
impl GraphClient for AgeClient {
    async fn execute(
        &self,
        cypher: &str,
        params: &Value,
        columns: Option<&[DeclaredColumn]>,
        bounds: QueryBounds,
        limit: Option<usize>,
    ) -> Result<GraphRowStream, GraphError> {
        // Positional binding: AGE's `cypher()` declares arity and nothing
        // else, so only the COUNT of declared columns reaches the SQL.
        // The UDTF already refuses the fallback on this backend
        // (`requires_declared_columns`); this is the defense in depth.
        let Some(declared) = columns else {
            return Err(GraphError::InvalidColumns {
                reason: "the age backend requires declared columns (its cypher() call \
                         must declare its result arity)"
                    .to_string(),
                accepted: super::value::ACCEPTED_TYPES,
            });
        };
        let arity = declared.len();
        // The fetch bound rides the OUTER statement as a real SQL LIMIT
        // (never inside the Cypher text): the backend and the wire are
        // bounded even when the caller passes no limit, a RowCapExceeded
        // costs max_rows + 1 rows instead of a full scan plus a full
        // drain before ROLLBACK, and the consumption-side checks below
        // stay as defense in depth. Same formula as labels(): a SQL
        // LIMIT at or under the cap is a clean stop; otherwise fetch one
        // past the cap to PROVE an overflow.
        let fetch = match limit {
            Some(l) if l <= bounds.max_rows => l,
            _ => bounds.max_rows.saturating_add(1),
        };
        let (sql, prepared) = self.build_sql(cypher, params, arity, fetch);
        let source = self.source_name.clone();
        let started = std::time::Instant::now();
        // Acquire OUTSIDE the client-side timeout window: acquire and
        // execution are sequential costs, and acquire_timeout already
        // bounds the queue with its own typed PoolTimedOut mapping.
        // Sharing one window would let 25s of pool contention leave a
        // 30s statement 10s of budget — reported as the query's timeout
        // with the server-side bound (the authoritative one) mostly
        // unspent.
        let acquired = self
            .pool
            .acquire()
            .await
            .map_err(|e| map_query_error(&source, bounds, &e))?;
        let run = async {
            // A MANUAL transaction on an explicitly acquired connection,
            // not sqlx's Transaction guard: cleanup must be able to
            // ROLLBACK FIRST and then DEALLOCATE on the SAME session — a
            // backend error (invalid Cypher, runtime error,
            // statement_timeout) puts the transaction in aborted state,
            // where DEALLOCATE itself is refused, while rollback does NOT
            // clear session-level prepared statements. Rollback-then-
            // deallocate is the only order that cleans up after backend
            // errors. The [`OpenTxnGuard`] covers what MANUAL cannot: a
            // scan future dropped mid-transaction still rolls back AND
            // deallocates before the connection re-enters the pool.
            let mut guard = OpenTxnGuard {
                conn: Some(acquired),
                prepared: prepared.clone(),
            };
            // The security boundary: the SERVER refuses writes in a read
            // transaction, whatever slipped past the keyword guard.
            guard
                .conn()
                .execute("BEGIN TRANSACTION READ ONLY")
                .await
                .map_err(|e| backend_error(&source, &e))?;
            let collected: Result<Vec<GraphRow>, GraphError> = async {
                let conn = guard.conn();
                // Server-side: runaway traversals die in the backend, not
                // in a client that gave up waiting.
                conn.execute(
                    format!(
                        "SET LOCAL statement_timeout = '{}ms'",
                        bounds.timeout.as_millis()
                    )
                    .as_str(),
                )
                .await
                .map_err(|e| backend_error(&source, &e))?;

                // Stream (simple protocol — see build_sql) and cap:
                // max_rows + 1 proves the overflow without buffering past
                // it.
                let mut rows: Vec<GraphRow> = Vec::new();
                let mut stream = conn.fetch_many(sqlx::raw_sql(&sql));
                while let Some(step) = stream.next().await {
                    let step = step.map_err(|e| map_query_error(&source, bounds, &e))?;
                    // PREPARE contributes a rowless result; only rows count.
                    let Either::Right(row) = step else { continue };
                    // A SQL LIMIT is a clean early stop — enough rows is
                    // success, unlike the cap below.
                    if limit.is_some_and(|l| rows.len() >= l) {
                        break;
                    }
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
                Ok(rows)
            }
            .await;
            // The clean path reclaims the connection — from here the
            // guard's drop no longer fires and cleanup runs inline, in
            // its required order.
            let mut conn = guard.defuse();
            // ROLLBACK unconditionally and FIRST: it clears an aborted
            // transaction (making the DEALLOCATE below executable) and a
            // read-only transaction had nothing to undo anyway. Its
            // RESULT is checked LAST — an early `?` here would skip the
            // DEALLOCATE below and leak the statement on the very success
            // path this cleanup exists for.
            let rollback = conn.execute("ROLLBACK").await;
            let mut dealloc = Ok(Default::default());
            if let Some(name) = &prepared {
                // On EVERY exit: prepared statements are SESSION-level,
                // the pid+seq names never reuse, and skipping this on an
                // error path would monotonically accumulate statements on
                // the pooled connection. Best-effort on the error path
                // (the original error stays the one reported — and when
                // the PREPARE itself failed there is nothing to drop); a
                // failed DEALLOCATE on the success path is itself an
                // error. Targeted, never ALL — sqlx's own statement cache
                // lives on the same connection.
                dealloc = conn.execute(format!("DEALLOCATE {name}").as_str()).await;
            }
            let rows = collected?;
            // Success path only, and only AFTER both cleanups ran:
            // rollback failure first (it happened first), then dealloc.
            rollback.map_err(|e| backend_error(&source, &e))?;
            dealloc.map_err(|e| backend_error(&source, &e))?;
            Ok(rows)
        };
        // The client-side timeout (and any drop of the scan future)
        // ABANDONS the run mid-flight; [`OpenTxnGuard`] then rolls the
        // open transaction back AND deallocates this call's prepared
        // statement before the connection re-enters the pool — the
        // guard is what RESCUES the session, so it must also be what
        // cleans it. The wrap covers only the transaction body (acquire
        // happened above, bounded by its own acquire_timeout), so the
        // server-side statement_timeout keeps its full budget and stays
        // the authoritative bound; this one covers a backend that stops
        // answering entirely.
        let rows = bounded(bounds.timeout, run).await?;
        tracing::debug!(
            source = %self.source_name,
            elapsed_ms = started.elapsed().as_millis() as u64,
            rows = rows.len(),
            "cypher_query executed"
        );
        Ok(futures::stream::iter(rows.into_iter().map(Ok)).boxed())
    }

    fn declared_columns_requirement(&self) -> Option<&'static str> {
        // AGE's `cypher()` call must declare its result arity — the
        // JSON-record fallback cannot exist on this backend. The text
        // also carries the positional-binding warning, because omitting
        // columns and mis-ordering them are the same caller mistake
        // family on this backend.
        Some(
            "'columns' is required on the age backend — declare the output columns \
             IN THE SAME ORDER AS YOUR RETURN CLAUSE (the binding is positional; \
             two same-typed columns declared out of order swap silently), \
             e.g. '{\"name\": \"string\", \"n\": \"node\"}'",
        )
    }

    async fn schema(
        &self,
        bounds: QueryBounds,
        limit: Option<usize>,
    ) -> Result<Vec<SchemaRow>, GraphError> {
        // Same bounds discipline as execute — a catalog read against a
        // wedged backend must not hang forever, and the design's "every
        // query is bounded" makes no catalog exemption: READ ONLY
        // transaction, server-side statement_timeout, row cap, and the
        // client-side wrap. The cap and the SQL LIMIT both ride the
        // query's own LIMIT clause, so at most max_rows + 1 label rows
        // ever cross the wire — never the whole catalog first.
        let fetch = match limit {
            // A SQL LIMIT at or under the cap is a clean early stop.
            Some(l) if l <= bounds.max_rows => l,
            // Otherwise fetch one past the cap to PROVE an overflow.
            _ => bounds.max_rows.saturating_add(1),
        };
        let source = self.source_name.clone();
        let run = async {
            let mut tx = self
                .pool
                .begin()
                .await
                .map_err(|e| backend_error(&source, &e))?;
            // Same protocol discipline as execute(): the SETs ride the
            // SIMPLE protocol as constant text. `sqlx::query` would take
            // the extended protocol and plant a server-side prepared
            // statement in the per-connection cache — keyed on the
            // interpolated text, so each distinct timeout value would
            // cache another copy on every pooled session.
            (&mut *tx)
                .execute("SET TRANSACTION READ ONLY")
                .await
                .map_err(|e| backend_error(&source, &e))?;
            (&mut *tx)
                .execute(
                    format!(
                        "SET LOCAL statement_timeout = '{}ms'",
                        bounds.timeout.as_millis()
                    )
                    .as_str(),
                )
                .await
                .map_err(|e| backend_error(&source, &e))?;
            // ag_catalog is the exact source for labels; AGE's own
            // catch-all labels (`_ag_label_vertex` / `_ag_label_edge`)
            // are implementation noise for an agent and are filtered.
            let rows = sqlx::query(
                "SELECT l.name, l.kind::text \
                 FROM ag_catalog.ag_label l \
                 JOIN ag_catalog.ag_graph g ON g.graphid = l.graph \
                 WHERE g.name = $1 AND l.name NOT LIKE '\\_ag\\_label\\_%' \
                 ORDER BY l.name LIMIT $2",
            )
            .bind(&self.graph_name)
            .bind(i64::try_from(fetch).unwrap_or(i64::MAX))
            .fetch_all(&mut *tx)
            .await
            .map_err(|e| map_query_error(&source, bounds, &e))?;
            if limit.is_none_or(|l| l > bounds.max_rows) && rows.len() > bounds.max_rows {
                return Err(GraphError::RowCapExceeded {
                    max_rows: bounds.max_rows,
                });
            }
            tx.rollback()
                .await
                .map_err(|e| backend_error(&source, &e))?;
            tracing::debug!(source = %source, labels = rows.len(), "graph_schema catalog read");
            rows.into_iter()
                .map(|row| {
                    let name: String = row.try_get(0).map_err(|e| backend_error(&source, &e))?;
                    let kind: String = row.try_get(1).map_err(|e| backend_error(&source, &e))?;
                    let kind = match kind.as_str() {
                        "v" => "vertex".to_string(),
                        "e" => "edge".to_string(),
                        other => other.to_string(),
                    };
                    // Names only, structurally: `ag_catalog` records label
                    // names and kinds and nothing else (AGE is
                    // schema-optional) — property discovery would mean
                    // scanning data, deliberately not done (design §Agent
                    // and LLM interaction).
                    Ok(SchemaRow {
                        label: name,
                        kind,
                        property: None,
                        property_type: None,
                    })
                })
                .collect()
        };
        bounded(bounds.timeout, run).await
    }
}

/// Render the `cypher()` invocation batch (see [`AgeClient::build_sql`]
/// for the protocol constraints that shape it). A free function so the
/// exact SQL text — quote doubling, dollar tags, arity columns, the
/// PREPARE/EXECUTE split — is pinned by unit tests without a pool.
fn build_cypher_sql(
    graph_name: &str,
    seq: u64,
    cypher: &str,
    params: &Value,
    arity: usize,
    fetch: usize,
) -> (String, Option<String>) {
    let tag = dollar_tag(cypher);
    let graph = graph_name.replace('\'', "''");
    let cols: Vec<String> = (0..arity)
        .map(|i| format!("c{i} ag_catalog.agtype"))
        .collect();
    let outs: Vec<String> = (0..arity).map(|i| format!("c{i}")).collect();
    let has_params = params.as_object().is_some_and(|m| !m.is_empty());
    if has_params {
        let name = format!("skq_p_{}_{seq}", std::process::id());
        let literal = params.to_string().replace('\'', "''");
        let batch = format!(
            "PREPARE {name}(ag_catalog.agtype) AS \
             SELECT {} FROM ag_catalog.cypher('{graph}', {tag}{cypher}{tag}, $1) \
             AS t({}) LIMIT {fetch}; \
             EXECUTE {name}('{literal}');",
            outs.join(", "),
            cols.join(", ")
        );
        (batch, Some(name))
    } else {
        (
            format!(
                "SELECT {} FROM ag_catalog.cypher('{graph}', {tag}{cypher}{tag}) \
                 AS t({}) LIMIT {fetch}",
                outs.join(", "),
                cols.join(", ")
            ),
            None,
        )
    }
}

/// A dollar-quote tag that provably does not occur in `text`: pick the
/// first `$skqN$` suffix the text does not contain — no escape rules to
/// get wrong.
fn dollar_tag(text: &str) -> String {
    // The collision test runs against `text + "$"`, not `text`: the
    // closing delimiter starts with `$`, so a query ENDING in the tag's
    // interior (e.g. `RETURN $skq` with a parameter named `skq`) forms
    // the full tag at the text/closing-tag boundary and would close the
    // literal early. Appending the `$` makes the check cover exactly the
    // stream Postgres scans.
    //
    // ONE pass: collect the tag-shaped substrings actually present, then
    // take the first free suffix — a probe stuffed with `$skq$ $skq1$ …`
    // costs one scan, not one scan per collision.
    let probe = format!("{text}$");
    let mut base_used = false;
    let mut used: std::collections::HashSet<u32> = std::collections::HashSet::new();
    for (i, _) in probe.match_indices("$skq") {
        let rest = &probe[i + "$skq".len()..];
        if let Some(end) = rest.find('$') {
            let digits = &rest[..end];
            if digits.is_empty() {
                base_used = true;
            } else if let Ok(n) = digits.parse::<u32>() {
                used.insert(n);
            }
        }
    }
    if !base_used {
        return "$skq$".to_string();
    }
    let mut n = 1u32;
    while used.contains(&n) {
        n += 1;
    }
    format!("$skq{n}$")
}

/// Env-var credential lookup, shared by every backend client so the
/// error wording (env-var NAME only, never a value) cannot drift per
/// backend.
pub(super) fn read_env(source: &str, field: &str, env: &str) -> Result<String, GraphError> {
    std::env::var(env).map_err(|_| GraphError::InvalidConfig {
        name: source.to_string(),
        reason: format!("{field}: ${env} is not set in this environment"),
    })
}

/// The client-side timeout wrap, shared by every backend client: the
/// SERVER-side timeout (statement_timeout / tx_timeout) is
/// authoritative; this covers a backend that stops answering entirely.
/// saturating_add: the config caps the timeout, but arithmetic here must
/// not be the thing that panics if that invariant ever moves.
pub(super) async fn bounded<T>(
    timeout: Duration,
    fut: impl Future<Output = Result<T, GraphError>>,
) -> Result<T, GraphError> {
    tokio::time::timeout(timeout.saturating_add(Duration::from_secs(5)), fut)
        .await
        .map_err(|_| GraphError::Timeout {
            seconds: timeout.as_secs(),
        })?
}

fn backend_error(source: &str, e: &sqlx::Error) -> GraphError {
    // db.message(), never e.to_string(), for database errors: Postgres
    // errors carry `position` and statement context — the caller's
    // Cypher, i.e. values — and sqlx's Display dropping them today is
    // an implementation detail of sqlx, not a guarantee of ours. Taking
    // message() makes the "query text never flows into errors" rule
    // THIS module's own (the 300-byte cap remains as backstop only).
    let (code, message) = match e {
        sqlx::Error::Database(db) => (db.code().map(|c| c.to_string()), db.message().to_string()),
        _ => (None, e.to_string()),
    };
    GraphError::backend(source, code.as_deref().unwrap_or("io"), &message)
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
    // A pool-acquire timeout is the queueing flavor of the same bound.
    if matches!(e, sqlx::Error::PoolTimedOut) {
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

        // The BOUNDARY case: a query ending in the tag's interior forms
        // the full tag against the closing delimiter's leading `$` —
        // `RETURN $skq` + `$…` would close `$skq$…$skq$` early. The probe
        // must scan text+"$", exactly the stream Postgres sees.
        let boundary = "RETURN $skq";
        let tag = dollar_tag(boundary);
        assert_ne!(tag, "$skq$", "boundary composition must bump the tag");
        assert!(!format!("{boundary}$").contains(&tag), "{tag}");
        let (sql, _) = build_cypher_sql("kg", 0, boundary, &serde_json::json!({}), 1, 11);
        // The emitted literal parses as ONE dollar-quoted string: the
        // closing tag is found exactly once past the opening.
        let open = sql.find(&tag).expect("opening tag");
        let close = sql[open + tag.len()..].find(&tag).expect("closing tag");
        assert_eq!(
            &sql[open + tag.len()..open + tag.len() + close],
            boundary,
            "the whole query is the literal body: {sql}"
        );
    }

    #[test]
    fn parameterless_sql_is_a_single_select_with_no_prepare() {
        let (sql, prepared) = build_cypher_sql(
            "kg",
            0,
            "MATCH (n) RETURN n.a, n.b",
            &serde_json::json!({}),
            2,
            101,
        );
        assert!(prepared.is_none(), "no params → nothing to DEALLOCATE");
        assert!(
            sql.starts_with("SELECT c0, c1 FROM ag_catalog.cypher('kg', $skq$"),
            "{sql}"
        );
        // The fetch bound is a REAL SQL LIMIT on the outer statement —
        // the backend and the wire are bounded even with no caller LIMIT.
        assert!(
            sql.ends_with("AS t(c0 ag_catalog.agtype, c1 ag_catalog.agtype) LIMIT 101"),
            "{sql}"
        );
        assert!(!sql.contains("PREPARE"), "{sql}");
    }

    #[test]
    fn parameterized_sql_prepares_executes_and_names_the_statement() {
        let (sql, prepared) = build_cypher_sql(
            "kg",
            7,
            "MATCH (n) WHERE n.x = $x RETURN n",
            &serde_json::json!({"x": 1}),
            1,
            5,
        );
        assert!(
            sql.contains("ag_catalog.agtype) LIMIT 5;"),
            "the PREPARE body carries the fetch LIMIT: {sql}"
        );
        let name = prepared.expect("params → PREPARE name to DEALLOCATE");
        assert!(name.starts_with("skq_p_"), "{name}");
        assert!(name.ends_with("_7"), "the seq uniquifies: {name}");
        assert!(
            sql.contains(&format!("PREPARE {name}(ag_catalog.agtype)")),
            "{sql}"
        );
        assert!(
            sql.contains(&format!("EXECUTE {name}('{{\"x\":1}}');")),
            "{sql}"
        );
    }

    #[test]
    fn hostile_values_stay_inside_their_literals() {
        // Graph names double their quotes; param values ride serde_json
        // encoding plus WHOLE-literal quote doubling — the SQL text can
        // never fall out of its string literal.
        let (sql, _) = build_cypher_sql(
            "kg",
            0,
            "RETURN $s",
            &serde_json::json!({"s": "O'Brien '; DROP TABLE x; --"}),
            1,
            10,
        );
        assert!(sql.contains("O''Brien ''; DROP TABLE x; --"), "{sql}");

        let (sql, _) = build_cypher_sql("g'name", 0, "RETURN 1", &serde_json::json!({}), 1, 10);
        assert!(sql.contains("cypher('g''name'"), "{sql}");
    }

    #[test]
    fn params_must_be_an_object() {
        assert!(validate_params(&serde_json::json!({"a": 1})).is_ok());
        let err = validate_params(&serde_json::json!([1])).unwrap_err();
        assert!(err.to_string().contains("an array"), "{err}");
    }
}
