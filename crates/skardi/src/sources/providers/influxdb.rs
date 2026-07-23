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

use super::CountSafeTable;

// Friendly `options` keys that Skardi treats specially for InfluxDB sources.
//
// These are the *Skardi-side* option names (as written in a `ctx` YAML), as
// opposed to the driver-side keys (`QUERY`, `HEADER_PREFIX`, …) imported from
// `datafusion-table-providers`. Centralising them here keeps the recognised
// vocabulary in one place so it can't silently drift between the parser, the
// docs, and the tests.

/// Full SQL backing the table, e.g. `SELECT * FROM cpu WHERE ...`.
const OPT_QUERY: &str = "query";
/// InfluxDB measurement name; shorthand expanded to `SELECT * FROM "<name>"`.
const OPT_MEASUREMENT: &str = "measurement";
/// Alias for [`OPT_MEASUREMENT`] — a measurement is InfluxDB's table.
const OPT_TABLE: &str = "table";
/// InfluxDB 3 database (a.k.a. bucket); sent as the `database` gRPC header.
/// Required — InfluxDB 3 needs it on every Flight call.
const OPT_DATABASE: &str = "database";
/// Name of an environment variable holding the API token (preferred — keeps
/// the secret out of the YAML config). Resolved at registration time.
const OPT_TOKEN_ENV: &str = "token_env";
/// Inline auth token; sent as `authorization: Bearer <token>`. Discouraged —
/// prefer [`OPT_TOKEN_ENV`] so the token isn't committed to config.
const OPT_TOKEN: &str = "token";
/// Prefix marking caller-supplied driver options forwarded verbatim.
const FLIGHT_PASSTHROUGH_PREFIX: &str = "flight.sql.";

/// Translate Skardi's `options` map into the Flight SQL driver's option keys.
///
/// Recognised options:
/// - `query` — full SQL backing the table (e.g. `SELECT * FROM cpu WHERE ...`).
/// - `measurement` / `table` — shorthand expanded to `SELECT * FROM "<name>"`.
///   One of `query` or `measurement`/`table` is required.
/// - `database` — InfluxDB 3 database (a.k.a. bucket); sent as the `database`
///   gRPC header that InfluxDB uses to pick the target database. Required
///   (unless supplied via a `flight.sql.header.database` passthrough key);
///   a missing or blank value is a configuration error.
/// - `token_env` — name of an environment variable holding the API token
///   (preferred; keeps the secret out of YAML). Sent as
///   `authorization: Bearer <token>`. Errors if the variable is unset or empty.
/// - `token` — inline API token (discouraged; prefer `token_env`). Takes effect
///   only when `token_env` is absent; a blank value is a configuration error.
/// - Any `flight.sql.*` key is forwarded verbatim and wins over the friendly
///   options above, so advanced setups (basic auth, custom headers) stay
///   reachable.
fn build_flight_options(
    name: &str,
    options: &HashMap<String, String>,
) -> Result<HashMap<String, String>> {
    let mut flight_opts: HashMap<String, String> = HashMap::new();

    // Resolve the backing query.
    let query = if let Some(q) = options.get(OPT_QUERY) {
        q.clone()
    } else if let Some(measurement) = options
        .get(OPT_MEASUREMENT)
        .or_else(|| options.get(OPT_TABLE))
    {
        // Double-quote the identifier and escape embedded quotes so measurement
        // names with special characters / reserved words are handled safely.
        format!("SELECT * FROM \"{}\"", measurement.replace('"', "\"\""))
    } else {
        return Err(anyhow!(
            "InfluxDB data source '{}' requires either a '{OPT_QUERY}' or a '{OPT_MEASUREMENT}' option",
            name
        ));
    };
    flight_opts.insert(QUERY.to_string(), query);

    // InfluxDB 3 selects the target database via the `database` gRPC header,
    // which it requires on *every* Flight call. Enforce it here so a missing or
    // blank database surfaces as a clear config error at registration rather
    // than an opaque Flight rejection at query time. Advanced setups can still
    // satisfy the requirement through the `flight.sql.header.database`
    // passthrough key (applied below), so we accept that as an alternative.
    let database_header = format!("{HEADER_PREFIX}database");
    match options.get(OPT_DATABASE) {
        Some(database) if !database.is_empty() => {
            flight_opts.insert(database_header.clone(), database.clone());
        }
        Some(_) => {
            return Err(anyhow!(
                "InfluxDB data source '{name}' has an empty '{OPT_DATABASE}' option; \
                 InfluxDB 3 requires the name of the database (bucket) to query"
            ));
        }
        None if options.contains_key(&database_header) => {
            // Supplied via the `flight.sql.*` passthrough; honoured below.
        }
        None => {
            return Err(anyhow!(
                "InfluxDB data source '{name}' requires a '{OPT_DATABASE}' option \
                 (the InfluxDB 3 database/bucket to query)"
            ));
        }
    }

    // Bearer-token auth → authorization header on the Flight calls.
    // Prefer `token_env` (names an environment variable) so the secret stays
    // out of the YAML config; fall back to an inline `token` for quick local
    // use, but warn against keeping it in committed config.
    let token = if let Some(token_env) = options.get(OPT_TOKEN_ENV) {
        let value = std::env::var(token_env).with_context(|| {
            format!(
                "Environment variable '{token_env}' (option '{OPT_TOKEN_ENV}') not found \
                 for InfluxDB source '{name}' token"
            )
        })?;
        // A set-but-empty variable would otherwise produce a malformed
        // `authorization: Bearer ` header and a confusing 401 at query time;
        // treat it as a configuration error instead.
        if value.is_empty() {
            return Err(anyhow!(
                "Environment variable '{token_env}' (option '{OPT_TOKEN_ENV}') is set but \
                 empty for InfluxDB source '{name}'; expected an API token"
            ));
        }
        Some(value)
    } else if let Some(token) = options.get(OPT_TOKEN) {
        if token.is_empty() {
            return Err(anyhow!(
                "InfluxDB source '{name}' has an empty '{OPT_TOKEN}' option; provide a \
                 non-empty API token or omit the option"
            ));
        }
        tracing::warn!(
            "InfluxDB source '{name}' uses an inline '{OPT_TOKEN}' option; prefer \
             '{OPT_TOKEN_ENV}' to keep the API token out of YAML config"
        );
        Some(token.clone())
    } else {
        None
    };
    if let Some(token) = token {
        flight_opts.insert(
            format!("{HEADER_PREFIX}authorization"),
            format!("Bearer {token}"),
        );
    }

    // Passthrough: caller-supplied flight.sql.* keys take precedence.
    for (key, value) in options {
        if key.starts_with(FLIGHT_PASSTHROUGH_PREFIX) {
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
            // Schema is inferred eagerly here (a live `GetFlightInfo` call), so
            // an unreachable endpoint, wrong database, or bad token fails server
            // startup. Spell out the likely causes so the boot failure is
            // actionable rather than a bare transport error.
            format!(
                "Failed to open InfluxDB Flight SQL table '{name}' at {connection_string} \
                 (schema is fetched at registration time); check that the endpoint is \
                 reachable, the '{OPT_DATABASE}' exists, and the token (if auth is enabled) \
                 is valid"
            )
        })?;

    // Wrapped so `SELECT count(*)` survives the upstream `enforce_schema`
    // empty-projection bug (see `CountSafeTable` in `providers/mod.rs`).
    let table = CountSafeTable {
        inner: Arc::new(table),
    };
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

    /// Like [`opts`], but injects a default `database` (which is now required)
    /// so query/token-translation tests can stay focused on what they assert.
    fn opts_with_db(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        let mut map = opts(pairs);
        map.entry(OPT_DATABASE.to_string())
            .or_insert_with(|| "metrics".to_string());
        map
    }

    #[test]
    fn measurement_shorthand_expands_to_select() {
        let flight = build_flight_options("cpu", &opts_with_db(&[("measurement", "cpu")])).unwrap();
        assert_eq!(flight.get(QUERY).unwrap(), "SELECT * FROM \"cpu\"");
    }

    #[test]
    fn table_is_accepted_as_an_alias_for_measurement() {
        let flight = build_flight_options("cpu", &opts_with_db(&[("table", "disk")])).unwrap();
        assert_eq!(flight.get(QUERY).unwrap(), "SELECT * FROM \"disk\"");
    }

    #[test]
    fn measurement_identifier_quotes_are_escaped() {
        let flight =
            build_flight_options("m", &opts_with_db(&[("measurement", "we\"ird")])).unwrap();
        assert_eq!(flight.get(QUERY).unwrap(), "SELECT * FROM \"we\"\"ird\"");
    }

    #[test]
    fn explicit_query_is_passed_through_verbatim() {
        let q = "SELECT host, usage FROM cpu WHERE usage > 0.5";
        let flight = build_flight_options("cpu", &opts_with_db(&[("query", q)])).unwrap();
        assert_eq!(flight.get(QUERY).unwrap(), q);
    }

    #[test]
    fn explicit_query_wins_over_measurement() {
        let flight = build_flight_options(
            "cpu",
            &opts_with_db(&[("query", "SELECT 1"), ("measurement", "cpu")]),
        )
        .unwrap();
        assert_eq!(flight.get(QUERY).unwrap(), "SELECT 1");
    }

    #[test]
    fn missing_database_is_an_error() {
        let err = build_flight_options("cpu", &opts(&[("measurement", "cpu")])).unwrap_err();
        assert!(
            err.to_string().contains("requires a 'database'"),
            "got {err}"
        );
    }

    #[test]
    fn empty_database_is_an_error() {
        let err = build_flight_options("cpu", &opts(&[("measurement", "cpu"), ("database", "")]))
            .unwrap_err();
        assert!(err.to_string().contains("empty 'database'"), "got {err}");
    }

    #[test]
    fn database_via_flight_sql_passthrough_satisfies_requirement() {
        // The advanced escape hatch: no friendly `database`, but the raw
        // `flight.sql.header.database` key provides it.
        let flight = build_flight_options(
            "cpu",
            &opts(&[
                ("measurement", "cpu"),
                ("flight.sql.header.database", "metrics"),
            ]),
        )
        .unwrap();
        assert_eq!(
            flight.get(&format!("{HEADER_PREFIX}database")).unwrap(),
            "metrics"
        );
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
        let flight = build_flight_options(
            "cpu",
            &opts_with_db(&[("measurement", "cpu"), ("token", "s3cr3t")]),
        )
        .unwrap();
        assert_eq!(
            flight
                .get(&format!("{HEADER_PREFIX}authorization"))
                .unwrap(),
            "Bearer s3cr3t"
        );
    }

    #[test]
    fn token_env_resolves_from_environment() {
        // Unique var name so the test is independent of others / the host env.
        let var = "SKARDI_TEST_INFLUX_TOKEN_ENV_OK";
        unsafe { std::env::set_var(var, "from-env-s3cr3t") };
        let flight = build_flight_options(
            "cpu",
            &opts_with_db(&[("measurement", "cpu"), ("token_env", var)]),
        )
        .unwrap();
        unsafe { std::env::remove_var(var) };
        assert_eq!(
            flight
                .get(&format!("{HEADER_PREFIX}authorization"))
                .unwrap(),
            "Bearer from-env-s3cr3t"
        );
    }

    #[test]
    fn token_env_wins_over_inline_token() {
        let var = "SKARDI_TEST_INFLUX_TOKEN_ENV_PRECEDENCE";
        unsafe { std::env::set_var(var, "env-wins") };
        let flight = build_flight_options(
            "cpu",
            &opts_with_db(&[
                ("measurement", "cpu"),
                ("token", "inline"),
                ("token_env", var),
            ]),
        )
        .unwrap();
        unsafe { std::env::remove_var(var) };
        assert_eq!(
            flight
                .get(&format!("{HEADER_PREFIX}authorization"))
                .unwrap(),
            "Bearer env-wins"
        );
    }

    #[test]
    fn missing_token_env_variable_is_an_error() {
        let err = build_flight_options(
            "cpu",
            &opts_with_db(&[
                ("measurement", "cpu"),
                ("token_env", "SKARDI_TEST_INFLUX_TOKEN_ENV_DEFINITELY_UNSET"),
            ]),
        )
        .unwrap_err();
        assert!(err.to_string().contains("not found"));
    }

    #[test]
    fn empty_token_env_variable_is_an_error() {
        let var = "SKARDI_TEST_INFLUX_TOKEN_ENV_EMPTY";
        unsafe { std::env::set_var(var, "") };
        let err = build_flight_options(
            "cpu",
            &opts_with_db(&[("measurement", "cpu"), ("token_env", var)]),
        )
        .unwrap_err();
        unsafe { std::env::remove_var(var) };
        assert!(err.to_string().contains("set but empty"), "got {err}");
    }

    #[test]
    fn empty_inline_token_is_an_error() {
        let err = build_flight_options(
            "cpu",
            &opts_with_db(&[("measurement", "cpu"), ("token", "")]),
        )
        .unwrap_err();
        assert!(err.to_string().contains("empty 'token'"), "got {err}");
    }

    #[test]
    fn raw_flight_sql_options_pass_through() {
        let flight = build_flight_options(
            "cpu",
            &opts_with_db(&[
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
            &opts_with_db(&[("measurement", "cpu"), ("flight.sql.query", "SELECT 42")]),
        )
        .unwrap();
        assert_eq!(flight.get(QUERY).unwrap(), "SELECT 42");
    }

    #[test]
    fn raw_authorization_header_overrides_token() {
        let flight = build_flight_options(
            "cpu",
            &opts_with_db(&[
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

    // ─── Integration tests (need a live InfluxDB 3 endpoint) ────────────
    //
    // Gated with `#[ignore]`; CI runs them via `cargo nextest -- --ignored`
    // after starting an InfluxDB 3 Core container and seeding the `metrics`
    // database (see .github/workflows/ci.yml and docs/influxdb/README.md).
    // The endpoint and database are read from env so the same test runs
    // locally and in CI.

    fn influx_url() -> String {
        std::env::var("INFLUXDB_URL").unwrap_or_else(|_| "http://127.0.0.1:8181".to_string())
    }

    fn influx_database() -> String {
        std::env::var("INFLUXDB_DATABASE").unwrap_or_else(|_| "metrics".to_string())
    }

    async fn register_ci_measurement(ctx: &mut SessionContext, name: &str, measurement: &str) {
        let options = opts(&[
            ("database", influx_database().as_str()),
            ("measurement", measurement),
        ]);
        register_influxdb_tables(ctx, name, &influx_url(), Some(&options))
            .await
            .unwrap_or_else(|e| panic!("register {name} failed: {e}"));
    }

    fn total_rows(batches: &[datafusion::arrow::record_batch::RecordBatch]) -> usize {
        batches.iter().map(|b| b.num_rows()).sum()
    }

    #[tokio::test]
    #[ignore]
    async fn integration_register_measurement_and_scan() {
        let mut ctx = SessionContext::new();
        register_ci_measurement(&mut ctx, "cpu", "cpu").await;

        let batches = ctx
            .sql("SELECT host, usage_user FROM cpu")
            .await
            .expect("plan query")
            .collect()
            .await
            .expect("collect");
        assert!(
            total_rows(&batches) >= 5,
            "expected at least 5 seeded cpu rows, got {}",
            total_rows(&batches)
        );
    }

    #[tokio::test]
    #[ignore]
    async fn integration_count_star_empty_projection() {
        // Regression: `count(*)` requests an empty projection, which trips an
        // upstream bug in the Flight provider's `enforce_schema`. The
        // CountSafeFlightTable wrapper must keep this from panicking and return
        // the correct row count.
        let mut ctx = SessionContext::new();
        register_ci_measurement(&mut ctx, "cpu", "cpu").await;

        let batches = ctx
            .sql("SELECT count(*) AS n FROM cpu")
            .await
            .expect("plan count(*)")
            .collect()
            .await
            .expect("collect count(*)");
        assert_eq!(total_rows(&batches), 1);
        let n = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Int64Array>()
            .expect("count is Int64")
            .value(0);
        assert!(n >= 5, "expected count >= 5 seeded rows, got {n}");
    }

    #[tokio::test]
    #[ignore]
    async fn integration_aggregation_pushes_through_flight() {
        let mut ctx = SessionContext::new();
        register_ci_measurement(&mut ctx, "cpu", "cpu").await;

        let batches = ctx
            .sql("SELECT count(*) AS n FROM cpu WHERE host = 'host1'")
            .await
            .expect("plan query")
            .collect()
            .await
            .expect("collect");
        assert_eq!(total_rows(&batches), 1);
    }

    #[tokio::test]
    #[ignore]
    async fn integration_register_with_explicit_query_option() {
        // Exercise the `query` option path (rather than `measurement`).
        let mut ctx = SessionContext::new();
        let options = opts(&[
            ("database", influx_database().as_str()),
            (
                "query",
                "SELECT host, usage_user FROM cpu WHERE usage_user > 50",
            ),
        ]);
        register_influxdb_tables(&mut ctx, "hot_cpu", &influx_url(), Some(&options))
            .await
            .expect("register with query option");

        let batches = ctx
            .sql("SELECT * FROM hot_cpu")
            .await
            .expect("plan query")
            .collect()
            .await
            .expect("collect");
        assert!(
            total_rows(&batches) >= 1,
            "expected at least one high-CPU row, got {}",
            total_rows(&batches)
        );
    }
}
