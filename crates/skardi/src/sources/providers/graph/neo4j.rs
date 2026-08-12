//! The milestone-2 backend: [`Neo4jClient`] — Neo4j over Bolt, via
//! `neo4rs` (design §Milestone 2).
//!
//! ## The access-mode spike, recorded (the milestone's hard precondition)
//!
//! The design gates this milestone on the driver being able to express
//! Bolt's READ access mode. Findings against `neo4rs` (verified in its
//! source at the pinned version):
//!
//! - **0.8.0 (latest stable) has no read-mode channel at all** — no
//!   `execute_read`, no `Operation`, and its `BEGIN` carries no mode.
//! - **0.9.0-rc.10 sends `mode: "r"` in the RUN message's extra map**
//!   when a query goes through `Graph::execute_read` — and for an
//!   AUTO-COMMIT transaction that is exactly where the Bolt protocol
//!   says the access mode belongs. This is the channel this client
//!   rides: every query is one auto-commit READ transaction.
//! - **Explicit transactions do NOT work for this**: the driver's
//!   `Begin` builder supports `with_mode`, but `Txn::new` never calls it
//!   (BEGIN always goes out in the default `"w"` mode), and the Bolt
//!   spec ignores the mode field on RUN *inside* an explicit
//!   transaction. So `start_txn_as(Operation::Read)` — behind the
//!   `unstable-bolt-protocol-impl-v2` feature — would add an unstable
//!   surface without adding enforcement. Not used; feature stays off.
//!
//! Because the enforcement unit is the auto-commit read transaction,
//! registration's **read-mode proof** probes that exact unit: it sends a
//! trivial write (`CREATE (n:…) DELETE n` — self-erasing even if it were
//! to execute, since auto-commit has no rollback to fall back on) through
//! `execute_read` and requires the server's ACCESS-MODE refusal
//! (`Neo.ClientError.Statement.AccessMode`, pinned live) — nothing else
//! passes: a write that executes fails registration, and so does any
//! OTHER error (a transient failure proves nothing about enforcement, so
//! it fails closed too; design §Security: "the milestone does not ship
//! on the keyword guard alone").
//!
//! ## Result decoding
//!
//! Bolt values decode into the same canonical JSON shapes the AGE client
//! produces (`value.rs` is the shared converter): nodes as
//! `{id, labels, properties}` (multi-label is native here — `labels` is
//! an array), relationships as `{id, start_id, end_id, label,
//! properties}`, paths as the flat `[node, rel, node, …]` alternation
//! reconstructed **in traversal order with directions resolved** from the
//! Bolt path's index list. Ids are Bolt's numeric ids stringified (the
//! default protocol negotiates Bolt 4.4, which predates `elementId()`;
//! design §Backend abstraction covers exactly this normalization).
//! Temporal values render as ISO-8601 text (durations through the
//! driver's signed (seconds, nanos) view — its `std::time::Duration`
//! conversion would wrap negatives), spatial points as small JSON
//! objects, byte arrays as lowercase hex — all JSON-representable, so a
//! `datetime()` property cannot fail a whole scan. Non-finite floats
//! decode to null (the same decision the AGE decoder made for agtype's
//! `Infinity`/`NaN` tokens).
//!
//! ## Column binding is BY NAME on this backend
//!
//! Bolt records carry their field names, so declared columns bind to
//! RETURN entries by NAME — the positional-order footgun documented for
//! AGE does not exist here, and a declared name missing from the record
//! is a typed error naming it. The JSON-`record` fallback (no declared
//! columns) packs the whole record as one JSON object per row; its keys
//! are emitted in SORTED order because the driver hands rows over as
//! hash maps (RETURN order is not recoverable through its public API).

use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime};
use futures::StreamExt;
use neo4rs::{BoltNode, BoltType, ConfigBuilder, Graph, Query};
use serde::Deserialize;
use serde::de::IntoDeserializer;
use serde_json::Value;

use super::client::{
    GraphClient, GraphRow, GraphRowStream, QueryBounds, SchemaRow, bounded, read_env,
};
use super::error::GraphError;
use super::value::DeclaredColumn;

/// Bolt PULL batch size. The wire-level bound for early stops: abandoning
/// a result after n rows costs at most one extra PULL batch, never the
/// whole result (Bolt streams incrementally, unlike the simple-protocol
/// full drain that pushed the AGE client to a real SQL LIMIT).
const FETCH_SIZE: usize = 1024;

/// The label used by the registration read-mode proof. Namespaced and
/// self-erasing: the probe query deletes what it creates in the same
/// statement, so even a non-enforcing server is left unchanged — and
/// registration then fails closed anyway.
const READ_PROOF_LABEL: &str = "__skardi_read_mode_probe__";

/// Neo4j over Bolt. Every query is one auto-commit READ-access-mode
/// transaction (see the module doc for why auto-commit is the sound
/// channel on the pinned driver).
pub struct Neo4jClient {
    graph: Graph,
    source_name: String,
}

impl std::fmt::Debug for Neo4jClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // neo4rs's Graph holds the credential; identity only here.
        f.debug_struct("Neo4jClient")
            .field("source_name", &self.source_name)
            .finish_non_exhaustive()
    }
}

impl Neo4jClient {
    /// Connect, verify reachability, and run the read-mode proof.
    /// Credentials arrive as env-var NAMES (values never in YAML);
    /// `database` selects a non-default database (`graph_name` in the
    /// config — omitted means the server's default).
    pub async fn connect(
        source_name: &str,
        connection_string: &str,
        database: Option<&str>,
        username_env: Option<&str>,
        password_env: Option<&str>,
        max_connections: u32,
        timeout: Duration,
    ) -> Result<Self, GraphError> {
        let user = match username_env {
            Some(env) => read_env(source_name, "username_env", env)?,
            None => String::new(),
        };
        let pass = match password_env {
            Some(env) => read_env(source_name, "password_env", env)?,
            None => String::new(),
        };
        let mut config = ConfigBuilder::default()
            .uri(connection_string)
            .user(user)
            .password(pass)
            .fetch_size(FETCH_SIZE)
            .max_connections(max_connections as usize)
            // Dialing is bounded by the same knob as queries — an
            // unreachable host must fail registration within the
            // configured bound, not the driver's own default.
            .connection_timeout(timeout);
        if let Some(db) = database {
            config = config.db(db);
        }
        let config = config.build().map_err(|e| GraphError::InvalidConfig {
            name: source_name.to_string(),
            reason: format!("neo4j driver config rejected: {e}"),
        })?;
        // Graph::connect only builds the pool — connections dial lazily,
        // so registration probes eagerly: a wrong URL, credential, or
        // database name fails HERE, source named (mod.rs's eager-fail
        // contract).
        let graph = Graph::connect(config).map_err(|e| map_driver_error(source_name, &e))?;
        let client = Self {
            graph,
            source_name: source_name.to_string(),
        };
        bounded(timeout, async {
            let mut rows = client
                .graph
                .execute_read(Query::new("RETURN 1".to_string()))
                .await
                .map_err(|e| map_driver_error(&client.source_name, &e))?;
            rows.next()
                .await
                .map_err(|e| map_driver_error(&client.source_name, &e))?;
            Ok(())
        })
        .await?;
        client.read_mode_proof(timeout).await?;
        Ok(client)
    }

    /// The registration read-mode proof (design §Milestone 2): send a
    /// trivial, self-erasing write through the SAME auto-commit read
    /// channel every later query uses, and require the server's
    /// ACCESS-MODE refusal. Fail-closed in every other outcome: a write
    /// that EXECUTES means read mode is not enforced, and any OTHER
    /// error (transient network failure, auth hiccup, a server that
    /// rejects the statement for unrelated reasons) proves nothing about
    /// enforcement — registration surfaces it instead of vacuously
    /// passing the security gate on it.
    async fn read_mode_proof(&self, timeout: Duration) -> Result<(), GraphError> {
        let probe = format!("CREATE (n:{READ_PROOF_LABEL}) DELETE n");
        let outcome = bounded(timeout, async {
            Ok(self.graph.execute_read(Query::new(probe)).await)
        })
        .await?;
        match outcome {
            // The one passing outcome, pinned live against Neo4j 5:
            // Neo.ClientError.Statement.AccessMode, "Writing in read
            // access mode not allowed".
            Err(neo4rs::Error::Neo4j(err)) if err.code().contains("AccessMode") => Ok(()),
            Err(other) => Err(GraphError::InvalidConfig {
                name: self.source_name.to_string(),
                reason: format!(
                    "the read-mode proof failed for a reason OTHER than the server's \
                     access-mode refusal — enforcement is unproven, so the source is \
                     not registered; underlying error: {}",
                    map_driver_error(&self.source_name, &other)
                ),
            }),
            Ok(_) => Err(GraphError::InvalidConfig {
                name: self.source_name.to_string(),
                reason: "the server EXECUTED a write inside a read-access-mode \
                         transaction — read-only cannot be enforced by this \
                         driver/server pair, so the source is not registered \
                         (the probe was self-erasing; the graph is unchanged)"
                    .to_string(),
            }),
        }
    }

    /// Build the driver query: params driver-bound (never interpolated —
    /// the design's normal road, which AGE alone cannot take), and the
    /// transaction timeout in the RUN extra so runaway traversals die
    /// server-side.
    fn build_query(cypher: &str, params: &Value, bounds: QueryBounds) -> Result<Query, GraphError> {
        let mut query = Query::new(cypher.to_string()).extra(
            "tx_timeout",
            i64::try_from(bounds.timeout.as_millis()).unwrap_or(i64::MAX),
        );
        if let Some(map) = params.as_object() {
            for (key, value) in map {
                query = query.param(key, json_to_bolt(value)?);
            }
        }
        Ok(query)
    }

    /// Consume up to `fetch` rows. Shared row-loop semantics with the AGE
    /// client: hitting `limit` is a clean early stop, `max_rows + 1` is
    /// the loud overflow proof.
    async fn collect_rows(
        &self,
        query: Query,
        columns: Option<&[DeclaredColumn]>,
        bounds: QueryBounds,
        limit: Option<usize>,
    ) -> Result<Vec<GraphRow>, GraphError> {
        let source = &self.source_name;
        // `map_query_error`, not `map_driver_error`: the server-side
        // tx_timeout surfaces mid-query and must map to the typed
        // Timeout naming the configured seconds.
        let mut stream = self
            .graph
            .execute_read(query)
            .await
            .map_err(|e| map_query_error(source, bounds, &e))?;
        let mut rows: Vec<GraphRow> = Vec::new();
        loop {
            if limit.is_some_and(|l| rows.len() >= l) {
                // Enough rows is success; tell the server to discard the
                // rest instead of abandoning the stream mid-result.
                let _ = stream.finish().await;
                break;
            }
            let Some(row) = stream
                .next()
                .await
                .map_err(|e| map_query_error(source, bounds, &e))?
            else {
                break;
            };
            if rows.len() >= bounds.max_rows {
                return Err(GraphError::RowCapExceeded {
                    max_rows: bounds.max_rows,
                });
            }
            let row_idx = rows.len();
            rows.push(match columns {
                Some(declared) => decode_declared(&row, declared, row_idx)?,
                None => vec![decode_record(&row, row_idx)?],
            });
        }
        Ok(rows)
    }

    /// One `db.schema.*` catalog query, flattened to [`SchemaRow`]s.
    async fn schema_rows(
        &self,
        query: String,
        kind: &'static str,
        bounds: QueryBounds,
    ) -> Result<Vec<SchemaRow>, GraphError> {
        let source = &self.source_name;
        let mut stream = self
            .graph
            .execute_read(Self::build_query(&query, &Value::Null, bounds)?)
            .await
            .map_err(|e| map_query_error(source, bounds, &e))?;
        let mut out = Vec::new();
        while let Some(row) = stream
            .next()
            .await
            .map_err(|e| map_query_error(source, bounds, &e))?
        {
            let labels: Vec<String> = match kind {
                "vertex" => {
                    let labels = row
                        .get::<Vec<String>>("nodeLabels")
                        .map_err(|e| schema_shape_error(source, &e))?;
                    if labels.is_empty() {
                        // Label-less nodes are legal on Neo4j and their
                        // node type still carries properties — one row
                        // with an empty label keeps them discoverable
                        // instead of silently vanishing from the roster.
                        vec![String::new()]
                    } else {
                        labels
                    }
                }
                // relType arrives as ":`KNOWS`" — strip to the name.
                _ => vec![strip_rel_type(
                    &row.get::<String>("relType")
                        .map_err(|e| schema_shape_error(source, &e))?,
                )],
            };
            // A label with no properties yields one row with a null
            // propertyName — keep it: the label's existence IS the
            // information. Decode ERRORS are not null: a YIELD-shape
            // drift must surface, exactly like nodeLabels above.
            let property: Option<String> = row
                .get("propertyName")
                .map_err(|e| schema_shape_error(source, &e))?;
            let property_type: Option<String> = row
                .get::<Option<Vec<String>>>("propertyTypes")
                .map_err(|e| schema_shape_error(source, &e))?
                .map(|types| types.join("|"));
            for label in labels {
                out.push(SchemaRow {
                    label,
                    kind: kind.to_string(),
                    property: property.clone(),
                    property_type: property_type.clone(),
                });
            }
        }
        Ok(out)
    }
}

#[async_trait]
impl GraphClient for Neo4jClient {
    async fn execute(
        &self,
        cypher: &str,
        params: &Value,
        columns: Option<&[DeclaredColumn]>,
        bounds: QueryBounds,
        limit: Option<usize>,
    ) -> Result<GraphRowStream, GraphError> {
        let query = Self::build_query(cypher, params, bounds)?;
        let rows = bounded(
            bounds.timeout,
            self.collect_rows(query, columns, bounds, limit),
        )
        .await?;
        Ok(futures::stream::iter(rows.into_iter().map(Ok)).boxed())
    }

    fn declared_columns_requirement(&self) -> Option<&'static str> {
        // Bolt records carry field names and need no declared arity —
        // the JSON-record fallback is native here.
        None
    }

    async fn schema(
        &self,
        bounds: QueryBounds,
        limit: Option<usize>,
    ) -> Result<Vec<SchemaRow>, GraphError> {
        // Engine-authored introspection — these fixed texts never pass
        // through the keyword guard (design: rejecting CALL must not
        // self-block discovery). The LIMIT bounds PROCEDURE rows; every
        // procedure row flattens to >= 1 schema rows, so max_rows + 1
        // procedure rows are enough to prove any flattened overflow.
        let fetch = bounds.max_rows.saturating_add(1);
        let node_q = format!(
            "CALL db.schema.nodeTypeProperties() \
             YIELD nodeLabels, propertyName, propertyTypes \
             RETURN nodeLabels, propertyName, propertyTypes LIMIT {fetch}"
        );
        let rel_q = format!(
            "CALL db.schema.relTypeProperties() \
             YIELD relType, propertyName, propertyTypes \
             RETURN relType, propertyName, propertyTypes LIMIT {fetch}"
        );
        let run = async {
            // The two catalog queries share no state — concurrent, on
            // two pooled connections.
            let (nodes, rels) = futures::try_join!(
                self.schema_rows(node_q, "vertex", bounds),
                self.schema_rows(rel_q, "edge", bounds),
            )?;
            let mut out = nodes;
            out.extend(rels);
            // The cap rule is the AGE client's, verbatim: a caller LIMIT
            // at or under the cap is a clean early stop (never a cap
            // error), and only an uncapped/over-cap read that actually
            // exceeds max_rows is the loud overflow.
            if limit.is_none_or(|l| l > bounds.max_rows) && out.len() > bounds.max_rows {
                return Err(GraphError::RowCapExceeded {
                    max_rows: bounds.max_rows,
                });
            }
            // The procedures publish no order; sort for a deterministic
            // agent-facing roster (AGE orders by name in SQL — and like
            // SQL, the sort runs before the LIMIT truncation).
            out.sort_by(|a, b| {
                (&a.kind, &a.label, &a.property).cmp(&(&b.kind, &b.label, &b.property))
            });
            if let Some(l) = limit {
                out.truncate(l);
            }
            Ok(out)
        };
        bounded(bounds.timeout, run).await
    }
}

/// Map a driver error to the taxonomy. Neo4j server errors carry their
/// code verbatim and a bounded message via [`GraphError::backend`]
/// (never `Display` of the whole error — messages can embed query text).
fn map_driver_error(source: &str, e: &neo4rs::Error) -> GraphError {
    match e {
        neo4rs::Error::Neo4j(err) => GraphError::backend(source, err.code(), err.message()),
        // IO/protocol errors carry addresses, never credentials or query
        // text — but bounded all the same.
        other => GraphError::backend(source, "driver", &other.to_string()),
    }
}

/// Like [`map_driver_error`] but with bounds in scope, so the server's
/// transaction timeout maps to the typed [`GraphError::Timeout`] naming
/// the configured seconds.
fn map_query_error(source: &str, bounds: QueryBounds, e: &neo4rs::Error) -> GraphError {
    // `contains`, not a suffix match: the live server answers a
    // client-set tx_timeout with `Transaction.
    // TransactionTimedOutClientConfiguration` (verified against Neo4j 5;
    // the server-config variant is plain `TransactionTimedOut`).
    if let neo4rs::Error::Neo4j(err) = e
        && err.code().contains("TransactionTimedOut")
    {
        return GraphError::Timeout {
            seconds: bounds.timeout.as_secs(),
        };
    }
    map_driver_error(source, e)
}

/// `db.schema.relTypeProperties()` spells relationship types as
/// ``:`KNOWS` `` — strip the decoration, keep the name.
fn strip_rel_type(raw: &str) -> String {
    raw.trim_start_matches(':').trim_matches('`').to_string()
}

/// A schema-procedure row that does not match the documented YIELD shape
/// — a server-version drift, surfaced with the field identity only.
fn schema_shape_error(source: &str, e: &neo4rs::DeError) -> GraphError {
    GraphError::backend(source, "schema-introspection", &e.to_string())
}

/// Decode one Bolt record against the declared columns, BY NAME.
fn decode_declared(
    row: &neo4rs::Row,
    columns: &[DeclaredColumn],
    row_idx: usize,
) -> Result<GraphRow, GraphError> {
    let mut values = Vec::with_capacity(columns.len());
    for (col_idx, col) in columns.iter().enumerate() {
        let bolt: BoltType = row.get(&col.name).map_err(|_| GraphError::MalformedCell {
            row: row_idx,
            column: col_idx,
            reason: format!(
                "the query returned no field named '{}' — on neo4j, declared \
                 columns bind to RETURN entries BY NAME; alias the entry \
                 (… AS {}) or fix the declaration",
                col.name, col.name
            ),
        })?;
        values.push(bolt_to_json(&bolt));
    }
    Ok(values)
}

/// Decode one Bolt record as the whole-record JSON object (the fallback
/// mode) — ONE deserialization pass over the row, then keys sorted (the
/// driver's row is a hash map, so RETURN order is not recoverable;
/// sorted beats nondeterministic).
fn decode_record(row: &neo4rs::Row, row_idx: usize) -> Result<Value, GraphError> {
    let map: neo4rs::BoltMap = row.to_strict().map_err(|e| GraphError::MalformedCell {
        row: row_idx,
        column: 0,
        reason: format!("record failed to decode: {e}"),
    })?;
    let mut entries: Vec<(&str, &BoltType)> = map
        .value
        .iter()
        .map(|(k, v)| (k.value.as_str(), v))
        .collect();
    entries.sort_unstable_by_key(|(k, _)| *k);
    let mut record = serde_json::Map::with_capacity(entries.len());
    for (key, bolt) in entries {
        record.insert(key.to_string(), bolt_to_json(bolt));
    }
    Ok(Value::Object(record))
}

/// One JSON parameter value → the driver's Bolt value. Recursion mirrors
/// JSON's own shape. Numbers must fit Bolt's 64-bit signed integers or
/// arrive as floats — a u64 beyond i64::MAX is a typed error, never a
/// silent narrowing to f64 (an equality filter on a narrowed id would
/// silently match nothing; AGE passes the exact literal through, and
/// divergence here must be loud).
fn json_to_bolt(v: &Value) -> Result<BoltType, GraphError> {
    Ok(match v {
        Value::Null => BoltType::Null(neo4rs::BoltNull),
        Value::Bool(b) => BoltType::Boolean(neo4rs::BoltBoolean::new(*b)),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                BoltType::Integer(neo4rs::BoltInteger::new(i))
            } else if n.is_u64() {
                return Err(GraphError::InvalidParams {
                    found: "an integer beyond the neo4j backend's signed 64-bit range \
                            (pass it as a string instead)"
                        .to_string(),
                });
            } else {
                BoltType::Float(neo4rs::BoltFloat::new(n.as_f64().unwrap_or(f64::NAN)))
            }
        }
        Value::String(s) => BoltType::String(neo4rs::BoltString::new(s)),
        Value::Array(items) => items
            .iter()
            .map(json_to_bolt)
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .collect(),
        Value::Object(map) => {
            let mut bolt = neo4rs::BoltMap::with_capacity(map.len());
            for (k, val) in map {
                bolt.put(neo4rs::BoltString::new(k), json_to_bolt(val)?);
            }
            BoltType::Map(bolt)
        }
    })
}

/// One Bolt value → the canonical JSON shape `value.rs` converts
/// (module doc: nodes/relationships/paths in the shared contract;
/// temporals as ISO-8601 text; non-finite floats as null).
fn bolt_to_json(v: &BoltType) -> Value {
    match v {
        BoltType::Null(_) => Value::Null,
        BoltType::Boolean(b) => Value::Bool(b.value),
        BoltType::Integer(i) => Value::from(i.value),
        // serde_json has no spelling for NaN/Infinity — null, the same
        // decision the agtype decoder made for AGE's bare tokens.
        BoltType::Float(f) => serde_json::Number::from_f64(f.value)
            .map(Value::Number)
            .unwrap_or(Value::Null),
        BoltType::String(s) => Value::from(s.value.clone()),
        BoltType::List(items) => Value::Array(items.iter().map(bolt_to_json).collect()),
        BoltType::Map(map) => bolt_map_json(map),
        BoltType::Node(n) => node_json(n),
        BoltType::Relation(r) => rel_json(
            r.id.value,
            r.start_node_id.value,
            r.end_node_id.value,
            &r.typ.value,
            &r.properties,
        ),
        // Outside a path a relationship always arrives bounded; an
        // unbounded one at the top level would be driver drift. Decoded
        // with no endpoints rather than failing the scan — the shape
        // check in value.rs will name it if a declared column meets it.
        BoltType::UnboundedRelation(r) => serde_json::json!({
            "id": r.id.value.to_string(),
            "label": r.typ.value,
            "properties": bolt_map_json(&r.properties),
        }),
        BoltType::Path(p) => path_json(p),
        BoltType::Date(d) => match NaiveDate::try_from(d) {
            Ok(date) => Value::from(date.format("%Y-%m-%d").to_string()),
            Err(_) => Value::Null,
        },
        BoltType::Time(t) => {
            let (time, offset): (NaiveTime, FixedOffset) = t.into();
            Value::from(format!("{}{}", time.format("%H:%M:%S%.f"), offset))
        }
        BoltType::LocalTime(t) => {
            let time: NaiveTime = t.into();
            Value::from(time.format("%H:%M:%S%.f").to_string())
        }
        BoltType::DateTime(dt) => match DateTime::<FixedOffset>::try_from(dt) {
            Ok(dt) => Value::from(dt.to_rfc3339()),
            Err(_) => Value::Null,
        },
        BoltType::LocalDateTime(dt) => match NaiveDateTime::try_from(dt) {
            Ok(dt) => Value::from(dt.format("%Y-%m-%dT%H:%M:%S%.f").to_string()),
            Err(_) => Value::Null,
        },
        BoltType::DateTimeZoneId(dt) => match NaiveDateTime::try_from(dt) {
            // The IANA zone id survives as a suffix (the chrono-tz
            // resolution is deliberately not a dependency here).
            Ok(naive) => Value::from(format!(
                "{}[{}]",
                naive.format("%Y-%m-%dT%H:%M:%S%.f"),
                dt.tz_id()
            )),
            Err(_) => Value::Null,
        },
        BoltType::Duration(_) => {
            // Through the driver's EXTERNAL serde view — a signed
            // (seconds, nanos) pair (months/days collapsed, saturating).
            // Its `From<BoltDuration> for std::time::Duration` is NOT
            // usable here: it casts `seconds as u64`, so a negative
            // Cypher duration (legal: duration({seconds: -30})) would
            // wrap to ~1.8e19 and render as garbage.
            match <(i64, i64)>::deserialize(v.into_deserializer()) {
                Ok((secs, nanos)) => {
                    let total = i128::from(secs) * 1_000_000_000 + i128::from(nanos);
                    let sign = if total < 0 { "-" } else { "" };
                    let abs = total.unsigned_abs();
                    Value::from(format!(
                        "{sign}PT{}.{:09}S",
                        abs / 1_000_000_000,
                        abs % 1_000_000_000
                    ))
                }
                Err(_) => Value::Null,
            }
        }
        BoltType::Point2D(p) => serde_json::json!({
            "srid": p.sr_id.value, "x": p.x.value, "y": p.y.value,
        }),
        BoltType::Point3D(p) => serde_json::json!({
            "srid": p.sr_id.value, "x": p.x.value, "y": p.y.value, "z": p.z.value,
        }),
        BoltType::Bytes(b) => Value::from(b.value.iter().fold(String::new(), |mut acc, byte| {
            use std::fmt::Write;
            let _ = write!(acc, "{byte:02x}");
            acc
        })),
    }
}

/// A Bolt node → the canonical vertex object. `labels` is an ARRAY (the
/// multi-label spelling `value.rs` accepts alongside AGE's single
/// `label`).
fn node_json(n: &BoltNode) -> Value {
    serde_json::json!({
        "id": n.id.value.to_string(),
        "labels": n.labels.iter().map(bolt_to_json).collect::<Vec<_>>(),
        "properties": bolt_map_json(&n.properties),
    })
}

/// The canonical bounded-relationship object — ONE spelling, shared by
/// the top-level Relation arm and path hops, so the contract `value.rs`
/// consumes cannot fork.
fn rel_json(id: i64, start_id: i64, end_id: i64, typ: &str, properties: &neo4rs::BoltMap) -> Value {
    serde_json::json!({
        "id": id.to_string(),
        "start_id": start_id.to_string(),
        "end_id": end_id.to_string(),
        "label": typ,
        "properties": bolt_map_json(properties),
    })
}

fn bolt_map_json(map: &neo4rs::BoltMap) -> Value {
    let mut out = serde_json::Map::with_capacity(map.value.len());
    for (k, v) in &map.value {
        out.insert(k.value.clone(), bolt_to_json(v));
    }
    Value::Object(out)
}

/// A Bolt path → the canonical flat alternation `[node, rel, node, …]`,
/// in TRAVERSAL order with directions resolved.
///
/// Bolt encodes a path as (nodes, unbound rels, indices): the start node
/// is `nodes[0]`, and each hop is an index pair — a 1-based, SIGNED
/// relationship index (sign = direction) followed by a 0-based node
/// index. The canonical shape wants bounded relationships (`start_id` /
/// `end_id`), so each hop resolves its endpoints from the nodes it
/// connects; a malformed element or index is decoded as null (which
/// `value.rs` rejects with the path's identity) rather than panicking on
/// driver drift. Everything is borrowed from the BoltPath's own lists —
/// the driver's accessor methods deep-clone every element and are
/// deliberately not used.
fn path_json(p: &neo4rs::BoltPath) -> Value {
    let mut nodes: Vec<&BoltNode> = Vec::with_capacity(p.nodes.len());
    for element in p.nodes.iter() {
        let BoltType::Node(n) = element else {
            return Value::Null; // non-node in the node list: drift
        };
        nodes.push(n);
    }
    let mut rels: Vec<&neo4rs::BoltUnboundedRelation> = Vec::with_capacity(p.rels.len());
    for element in p.rels.iter() {
        let BoltType::UnboundedRelation(r) = element else {
            return Value::Null;
        };
        rels.push(r);
    }
    let mut indices: Vec<i64> = Vec::with_capacity(p.indices.len());
    for element in p.indices.iter() {
        let BoltType::Integer(i) = element else {
            return Value::Null;
        };
        indices.push(i.value);
    }
    let Some(&start) = nodes.first() else {
        return Value::Null; // no start node: not a path
    };
    let mut elements: Vec<Value> = Vec::with_capacity(indices.len() + 1);
    elements.push(node_json(start));
    let mut current = start;
    for hop in indices.chunks(2) {
        let [rel_idx, node_idx] = hop else {
            return Value::Null; // odd index list: not a path
        };
        let rel_pos = rel_idx.unsigned_abs() as usize;
        let (Some(rel), Ok(node_pos)) = (
            rel_pos.checked_sub(1).and_then(|i| rels.get(i).copied()),
            usize::try_from(*node_idx),
        ) else {
            return Value::Null;
        };
        let Some(&next) = nodes.get(node_pos) else {
            return Value::Null;
        };
        // Positive index: traversed with the relationship (current →
        // next); negative: against it.
        let (start_id, end_id) = if *rel_idx >= 0 {
            (current.id.value, next.id.value)
        } else {
            (next.id.value, current.id.value)
        };
        elements.push(rel_json(
            rel.id.value,
            start_id,
            end_id,
            &rel.typ.value,
            &rel.properties,
        ));
        elements.push(node_json(next));
        current = next;
    }
    Value::Array(elements)
}

#[cfg(test)]
mod tests {
    use super::*;
    use neo4rs::{BoltBoolean, BoltInteger, BoltList, BoltMap, BoltPath, BoltString};

    fn bolt_node(id: i64, labels: &[&str], props: &[(&str, &str)]) -> BoltNode {
        let mut label_list = BoltList::new();
        for l in labels {
            label_list.push(BoltType::String(BoltString::new(l)));
        }
        let mut properties = BoltMap::new();
        for (k, v) in props {
            properties.put(BoltString::new(k), BoltType::String(BoltString::new(v)));
        }
        BoltNode::new(BoltInteger::new(id), label_list, properties)
    }

    fn bolt_rel(id: i64, typ: &str) -> neo4rs::BoltUnboundedRelation {
        neo4rs::BoltUnboundedRelation::new(
            BoltInteger::new(id),
            BoltString::new(typ),
            BoltMap::new(),
        )
    }

    #[test]
    fn scalars_decode_to_their_json_kinds_and_specials_go_null() {
        assert_eq!(bolt_to_json(&BoltType::Null(neo4rs::BoltNull)), Value::Null);
        assert_eq!(
            bolt_to_json(&BoltType::Boolean(BoltBoolean::new(true))),
            Value::Bool(true)
        );
        assert_eq!(
            bolt_to_json(&BoltType::Integer(BoltInteger::new(-7))),
            serde_json::json!(-7)
        );
        assert_eq!(
            bolt_to_json(&BoltType::String(BoltString::new("颱風 café ☔"))),
            serde_json::json!("颱風 café ☔")
        );
        // The agtype decoder's float-specials decision, honored here too.
        for special in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            assert_eq!(
                bolt_to_json(&BoltType::Float(neo4rs::BoltFloat::new(special))),
                Value::Null
            );
        }
    }

    #[test]
    fn nodes_carry_all_labels_and_stringified_ids() {
        let v = bolt_to_json(&BoltType::Node(bolt_node(
            42,
            &["Person", "Admin"],
            &[("name", "ada")],
        )));
        assert_eq!(v["id"], "42");
        assert_eq!(v["labels"], serde_json::json!(["Person", "Admin"]));
        assert_eq!(v["properties"]["name"], "ada");
    }

    #[test]
    fn paths_reconstruct_traversal_order_and_direction() {
        // (a)-[:KNOWS]->(b)<-[:KNOWS]-(c), traversed a → b → c: hop 2
        // goes AGAINST its relationship, so its index is negative and
        // its endpoints must come out (c, b).
        let a = bolt_node(1, &["P"], &[]);
        let b = bolt_node(2, &["P"], &[]);
        let c = bolt_node(3, &["P"], &[]);
        let mut nodes = BoltList::new();
        for n in [&a, &b, &c] {
            nodes.push(BoltType::Node(n.clone()));
        }
        let mut rels = BoltList::new();
        rels.push(BoltType::UnboundedRelation(bolt_rel(10, "KNOWS")));
        rels.push(BoltType::UnboundedRelation(bolt_rel(11, "KNOWS")));
        let mut indices = BoltList::new();
        for i in [1i64, 1, -2, 2] {
            indices.push(BoltType::Integer(BoltInteger::new(i)));
        }
        let path = BoltPath {
            nodes,
            rels,
            indices,
        };
        let v = bolt_to_json(&BoltType::Path(path));
        let elements = v.as_array().expect("alternation array");
        assert_eq!(elements.len(), 5, "3 nodes + 2 rels");
        assert_eq!(elements[0]["id"], "1");
        assert_eq!(elements[1]["start_id"], "1");
        assert_eq!(elements[1]["end_id"], "2");
        assert_eq!(elements[2]["id"], "2");
        // The reversed hop: traversal b → c, relationship c → b.
        assert_eq!(elements[3]["start_id"], "3");
        assert_eq!(elements[3]["end_id"], "2");
        assert_eq!(elements[4]["id"], "3");
    }

    #[test]
    fn zero_hop_paths_are_one_node() {
        let mut nodes = BoltList::new();
        nodes.push(BoltType::Node(bolt_node(1, &["P"], &[])));
        let path = BoltPath {
            nodes,
            rels: BoltList::new(),
            indices: BoltList::new(),
        };
        let v = bolt_to_json(&BoltType::Path(path));
        assert_eq!(v.as_array().map(Vec::len), Some(1));
    }

    #[test]
    fn malformed_path_indices_decode_null_never_panic() {
        // rel index 1 with no rels — driver drift, not a panic.
        let mut nodes = BoltList::new();
        nodes.push(BoltType::Node(bolt_node(1, &["P"], &[])));
        nodes.push(BoltType::Node(bolt_node(2, &["P"], &[])));
        let mut indices = BoltList::new();
        for i in [1i64, 1] {
            indices.push(BoltType::Integer(BoltInteger::new(i)));
        }
        let path = BoltPath {
            nodes,
            rels: BoltList::new(),
            indices,
        };
        assert_eq!(bolt_to_json(&BoltType::Path(path)), Value::Null);
    }

    #[test]
    fn temporals_render_iso_text() {
        let date = NaiveDate::from_ymd_opt(2026, 8, 13).unwrap();
        assert_eq!(
            bolt_to_json(&BoltType::Date(date.into())),
            serde_json::json!("2026-08-13")
        );
        let dt = DateTime::parse_from_rfc3339("2026-08-13T09:30:00+08:00").unwrap();
        assert_eq!(
            bolt_to_json(&BoltType::DateTime(dt.into())),
            serde_json::json!("2026-08-13T09:30:00+08:00")
        );
        let std_dur = std::time::Duration::new(90, 500_000_000);
        assert_eq!(
            bolt_to_json(&BoltType::Duration(std_dur.into())),
            serde_json::json!("PT90.500000000S")
        );
    }

    #[test]
    fn negative_durations_render_signed_never_wrapped() {
        // duration({seconds: -30}) is legal Cypher; the driver's own
        // std::time::Duration conversion would wrap it to ~1.8e19.
        let neg = neo4rs::BoltDuration::new(0.into(), 0.into(), (-30i64).into(), 0.into());
        assert_eq!(
            bolt_to_json(&BoltType::Duration(neg)),
            serde_json::json!("-PT30.000000000S")
        );
        // Mixed sign resolves through total nanoseconds: -1s + 0.25s.
        let mixed =
            neo4rs::BoltDuration::new(0.into(), 0.into(), (-1i64).into(), 250_000_000i64.into());
        assert_eq!(
            bolt_to_json(&BoltType::Duration(mixed)),
            serde_json::json!("-PT0.750000000S")
        );
    }

    #[test]
    fn params_convert_shape_faithfully_and_overflow_is_typed() {
        let params = serde_json::json!({
            "s": "it's", "i": 3, "f": 2.5, "b": true, "nul": null,
            "list": [1, 2], "map": {"k": "v"},
        });
        let map = params.as_object().unwrap();
        assert!(matches!(
            json_to_bolt(&map["s"]).unwrap(),
            BoltType::String(_)
        ));
        assert!(matches!(
            json_to_bolt(&map["i"]).unwrap(),
            BoltType::Integer(_)
        ));
        assert!(matches!(
            json_to_bolt(&map["f"]).unwrap(),
            BoltType::Float(_)
        ));
        assert!(matches!(
            json_to_bolt(&map["b"]).unwrap(),
            BoltType::Boolean(_)
        ));
        assert!(matches!(
            json_to_bolt(&map["nul"]).unwrap(),
            BoltType::Null(_)
        ));
        let BoltType::List(l) = json_to_bolt(&map["list"]).unwrap() else {
            panic!("list");
        };
        assert_eq!(l.len(), 2);
        let BoltType::Map(m) = json_to_bolt(&map["map"]).unwrap() else {
            panic!("map");
        };
        assert_eq!(m.len(), 1);
        // A u64 beyond i64::MAX must be a typed refusal, never a silent
        // narrowing to f64 (an equality filter would then silently miss).
        let big = serde_json::json!(u64::MAX);
        let err = json_to_bolt(&big).unwrap_err();
        assert!(err.to_string().contains("64-bit"), "{err}");
        // Nested overflows are caught too.
        let nested = serde_json::json!({"ids": [1, u64::MAX]});
        assert!(json_to_bolt(&nested).is_err());
    }

    #[test]
    fn rel_type_decoration_strips() {
        assert_eq!(strip_rel_type(":`KNOWS`"), "KNOWS");
        assert_eq!(strip_rel_type("KNOWS"), "KNOWS");
    }
}
