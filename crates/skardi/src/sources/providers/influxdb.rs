//! InfluxDB 3 data source provider.
//!
//! InfluxDB 3 (Core / Enterprise) is itself built on Apache Arrow + DataFusion
//! and exposes an Arrow **Flight SQL** endpoint for queries. Because the query
//! engine lives *inside* InfluxDB, Skardi does not need a bespoke
//! `TableProvider`: it connects over the wire using the generic Flight SQL
//! provider shipped by `datafusion-table-providers`. This module's only job is
//! to translate Skardi's friendly `options` map (database / token / query /
//! measurement) into the Flight SQL driver's option keys and register the
//! resulting `FlightTable` — which already implements DataFusion's
//! `TableProvider` — into the session context.
//!
//! Access is **read-only**: Flight SQL serves `SELECT`s only. Writes to
//! InfluxDB go through the line-protocol ingest API, which is out of scope for
//! a SQL query engine, so InfluxDB sources never participate in CRUD or job
//! destinations.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context, Result, anyhow};
use datafusion::prelude::SessionContext;
use datafusion_table_providers::flight::FlightTableFactory;
use datafusion_table_providers::flight::sql::{FlightSqlDriver, HEADER_PREFIX, QUERY};

/// Translate Skardi's `options` map into the Flight SQL driver's option keys.
///
/// Recognised options:
/// - `query` — full SQL backing the table (e.g. `SELECT * FROM cpu WHERE ...`).
/// - `measurement` / `table` — shorthand expanded to `SELECT * FROM "<name>"`.
///   One of `query` or `measurement`/`table` is required.
/// - `database` — InfluxDB 3 database (a.k.a. bucket); sent as the `database`
///   gRPC header that InfluxDB uses to pick the target database.
/// - `token` — auth token; sent as `authorization: Bearer <token>`.
/// - Any `flight.sql.*` key is forwarded verbatim and wins over the friendly
///   options above, so advanced setups (basic auth, custom headers) stay
///   reachable.
fn build_flight_options(
    name: &str,
    options: &HashMap<String, String>,
) -> Result<HashMap<String, String>> {
    let mut flight_opts: HashMap<String, String> = HashMap::new();

    // Resolve the backing query.
    let query = if let Some(q) = options.get("query") {
        q.clone()
    } else if let Some(measurement) = options.get("measurement").or_else(|| options.get("table")) {
        // Double-quote the identifier and escape embedded quotes so measurement
        // names with special characters / reserved words are handled safely.
        format!("SELECT * FROM \"{}\"", measurement.replace('"', "\"\""))
    } else {
        return Err(anyhow!(
            "InfluxDB data source '{}' requires either a 'query' or a 'measurement' option",
            name
        ));
    };
    flight_opts.insert(QUERY.to_string(), query);

    // InfluxDB 3 selects the target database via the `database` gRPC header.
    if let Some(database) = options.get("database") {
        flight_opts.insert(format!("{HEADER_PREFIX}database"), database.clone());
    }

    // Bearer-token auth → authorization header on the Flight calls.
    if let Some(token) = options.get("token") {
        flight_opts.insert(
            format!("{HEADER_PREFIX}authorization"),
            format!("Bearer {token}"),
        );
    }

    // Passthrough: caller-supplied flight.sql.* keys take precedence.
    for (key, value) in options {
        if key.starts_with("flight.sql.") {
            flight_opts.insert(key.clone(), value.clone());
        }
    }

    Ok(flight_opts)
}

/// Register an InfluxDB 3 measurement (or arbitrary SQL query) as a Skardi table
/// backed by the source's Arrow Flight SQL endpoint.
///
/// `connection_string` is the Flight gRPC endpoint URL, e.g.
/// `http://localhost:8181` for a local InfluxDB 3 Core instance. The table is
/// registered under `name`; its Arrow schema is inferred at registration time
/// from the server's `GetFlightInfo` response, matching how the other database
/// providers connect eagerly during config load.
pub async fn register_influxdb_tables(
    session_ctx: &mut SessionContext,
    name: &str,
    connection_string: &str,
    options: Option<&HashMap<String, String>>,
) -> Result<()> {
    let options = options.ok_or_else(|| {
        anyhow!(
            "InfluxDB data source '{}' requires options (database, and query or measurement)",
            name
        )
    })?;

    let flight_opts = build_flight_options(name, options)?;

    tracing::info!(
        "Registering InfluxDB Flight SQL table '{}' against endpoint {}",
        name,
        connection_string
    );

    // `persistent_headers` propagates our headers (database + bearer token)
    // from the GetFlightInfo call onto the subsequent DoGet data fetch.
    // InfluxDB 3 requires the `database` header — and the bearer token, when
    // auth is enabled — on *every* Flight call, so without this the data
    // stream would be rejected even though schema discovery succeeded.
    let driver = FlightSqlDriver::new().with_persistent_headers(true);
    let factory = FlightTableFactory::new(Arc::new(driver));
    let table = factory
        .open_table(connection_string.to_string(), flight_opts)
        .await
        .with_context(|| {
            format!(
                "Failed to open InfluxDB Flight SQL table '{}' at {}",
                name, connection_string
            )
        })?;

    session_ctx
        .register_table(name, Arc::new(table))
        .with_context(|| format!("Failed to register InfluxDB table '{}'", name))?;

    tracing::info!("Successfully registered InfluxDB table: {}", name);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn opts(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn measurement_shorthand_expands_to_select() {
        let flight = build_flight_options("cpu", &opts(&[("measurement", "cpu")])).unwrap();
        assert_eq!(flight.get(QUERY).unwrap(), "SELECT * FROM \"cpu\"");
    }

    #[test]
    fn table_is_accepted_as_an_alias_for_measurement() {
        let flight = build_flight_options("cpu", &opts(&[("table", "disk")])).unwrap();
        assert_eq!(flight.get(QUERY).unwrap(), "SELECT * FROM \"disk\"");
    }

    #[test]
    fn measurement_identifier_quotes_are_escaped() {
        let flight = build_flight_options("m", &opts(&[("measurement", "we\"ird")])).unwrap();
        assert_eq!(flight.get(QUERY).unwrap(), "SELECT * FROM \"we\"\"ird\"");
    }

    #[test]
    fn explicit_query_is_passed_through_verbatim() {
        let q = "SELECT host, usage FROM cpu WHERE usage > 0.5";
        let flight = build_flight_options("cpu", &opts(&[("query", q)])).unwrap();
        assert_eq!(flight.get(QUERY).unwrap(), q);
    }

    #[test]
    fn explicit_query_wins_over_measurement() {
        let flight = build_flight_options(
            "cpu",
            &opts(&[("query", "SELECT 1"), ("measurement", "cpu")]),
        )
        .unwrap();
        assert_eq!(flight.get(QUERY).unwrap(), "SELECT 1");
    }

    #[test]
    fn missing_query_and_measurement_is_an_error() {
        let err = build_flight_options("cpu", &opts(&[("database", "metrics")])).unwrap_err();
        assert!(err.to_string().contains("requires either a 'query'"));
    }

    #[test]
    fn database_maps_to_grpc_header() {
        let flight = build_flight_options(
            "cpu",
            &opts(&[("measurement", "cpu"), ("database", "metrics")]),
        )
        .unwrap();
        assert_eq!(
            flight.get(&format!("{HEADER_PREFIX}database")).unwrap(),
            "metrics"
        );
    }

    #[test]
    fn token_maps_to_bearer_authorization_header() {
        let flight =
            build_flight_options("cpu", &opts(&[("measurement", "cpu"), ("token", "s3cr3t")]))
                .unwrap();
        assert_eq!(
            flight
                .get(&format!("{HEADER_PREFIX}authorization"))
                .unwrap(),
            "Bearer s3cr3t"
        );
    }

    #[test]
    fn raw_flight_sql_options_pass_through() {
        let flight = build_flight_options(
            "cpu",
            &opts(&[
                ("measurement", "cpu"),
                ("flight.sql.username", "admin"),
                ("flight.sql.header.custom", "1"),
            ]),
        )
        .unwrap();
        assert_eq!(flight.get("flight.sql.username").unwrap(), "admin");
        assert_eq!(flight.get("flight.sql.header.custom").unwrap(), "1");
    }

    #[test]
    fn full_option_set_produces_exactly_the_expected_keys() {
        let flight = build_flight_options(
            "cpu",
            &opts(&[
                ("measurement", "cpu"),
                ("database", "metrics"),
                ("token", "s3cr3t"),
            ]),
        )
        .unwrap();

        // Friendly keys must be translated, never leaked verbatim.
        assert!(!flight.contains_key("measurement"));
        assert!(!flight.contains_key("database"));
        assert!(!flight.contains_key("token"));

        let mut keys: Vec<&str> = flight.keys().map(String::as_str).collect();
        keys.sort_unstable();
        assert_eq!(
            keys,
            vec![
                "flight.sql.header.authorization",
                "flight.sql.header.database",
                "flight.sql.query",
            ]
        );
    }

    #[test]
    fn raw_flight_sql_query_overrides_measurement_derived_query() {
        // `flight.sql.query` IS the QUERY key, so a raw value wins over the
        // measurement-derived one (passthrough runs last).
        let flight = build_flight_options(
            "cpu",
            &opts(&[("measurement", "cpu"), ("flight.sql.query", "SELECT 42")]),
        )
        .unwrap();
        assert_eq!(flight.get(QUERY).unwrap(), "SELECT 42");
    }

    #[test]
    fn raw_authorization_header_overrides_token() {
        let flight = build_flight_options(
            "cpu",
            &opts(&[
                ("measurement", "cpu"),
                ("token", "friendly"),
                ("flight.sql.header.authorization", "Bearer raw"),
            ]),
        )
        .unwrap();
        assert_eq!(
            flight
                .get(&format!("{HEADER_PREFIX}authorization"))
                .unwrap(),
            "Bearer raw"
        );
    }

    #[tokio::test]
    async fn register_without_options_errors() {
        let mut ctx = SessionContext::new();
        let err = register_influxdb_tables(&mut ctx, "cpu", "http://localhost:8181", None)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("requires options"));
    }

    #[tokio::test]
    async fn register_without_query_or_measurement_errors_before_connecting() {
        // Only `database` is given — the option-validation error must fire
        // before any network call, so this is safe to run offline.
        let mut ctx = SessionContext::new();
        let options = opts(&[("database", "metrics")]);
        let err = register_influxdb_tables(
            &mut ctx,
            "cpu",
            // Deliberately unroutable; we must fail before ever dialing it.
            "http://127.0.0.1:1",
            Some(&options),
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("requires either a 'query'"));
    }
}
