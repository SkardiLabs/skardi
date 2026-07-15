//! ClickHouse data source provider.
//!
//! ClickHouse is a columnar OLAP database with an HTTP query interface. This
//! module drives the ClickHouse table provider shipped by
//! `datafusion-table-providers`: filters, projections, and limits are unparsed
//! back to ClickHouse SQL and executed server-side, so scans stream only the
//! rows the query actually needs.
//!
//! Access is **read-only**. ClickHouse has no transactional UPDATE/DELETE —
//! mutations (`ALTER TABLE ... UPDATE/DELETE`) are asynchronous background
//! rewrites, and a mid-stream INSERT failure leaves partial parts visible.
//! Neither matches the semantics Skardi's write path promises, so ClickHouse
//! sources never participate in CRUD or job destinations.
//!
//! Two registration modes, mirroring the other SQL providers:
//! - **Table mode** (default): one ClickHouse table registered under `name`.
//! - **Catalog mode**: every table across the server's non-system databases is
//!   registered under the `name` catalog as `name.<database>.<table>`.

use crate::sources::DataSourceType;
use crate::sources::hierarchy::{
    HierarchyLevel, SourceLabel, build_catalog, parse_allowed_schemas, retry_with_timeout,
};
use anyhow::{Context, Result, anyhow};
use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::Session;
use datafusion::common::Statistics;
use datafusion::datasource::TableProvider;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::prelude::SessionContext;
use datafusion::sql::TableReference;
use datafusion_table_providers::clickhouse::ClickHouseTableFactory;
use datafusion_table_providers::sql::db_connection_pool::clickhousepool::ClickHouseConnectionPool;
use datafusion_table_providers::sql::db_connection_pool::dbconnection::AsyncDbConnection;
use secrecy::SecretString;
use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

// Skardi-side `options` keys (as written in a ctx YAML). Centralised so the
// recognised vocabulary can't drift between the parser, the docs, and the
// tests.

/// ClickHouse table to register (required in table mode).
const OPT_TABLE: &str = "table";
/// ClickHouse database holding the table (table mode; optional — defaults to
/// the server's default database, usually `default`).
const OPT_DATABASE: &str = "database";
/// Name of an environment variable holding the username. ClickHouse's
/// out-of-the-box `default` user needs no credentials, so both are optional.
const OPT_USER_ENV: &str = "user_env";
/// Name of an environment variable holding the password.
const OPT_PASS_ENV: &str = "pass_env";

/// Register ClickHouse tables or a whole server (catalog) into a DataFusion
/// [`SessionContext`].
///
/// Single-table mode (default) registers one table under `name`. Catalog mode
/// registers one provider per table across all non-system databases.
///
/// # Arguments
/// * `session_ctx` - DataFusion session context to register tables into
/// * `name` - Name to register the table (table mode) or catalog (catalog mode) as
/// * `connection_string` - ClickHouse HTTP endpoint, e.g. `http://localhost:8123`.
///   Credentials must NOT be embedded in the URL; use `user_env` / `pass_env`.
/// * `options` - Optional configuration (see below)
/// * `hierarchy_level` - [`HierarchyLevel::Table`] (default) or [`HierarchyLevel::Catalog`]
///
/// # Options
/// * `table` - Table name (required in table mode)
/// * `database` - Database name (optional in table mode; defaults to the
///   server's default database)
/// * `allowed_schemas` - Comma-separated database allow-list (catalog mode only)
/// * `user_env` - Environment variable name for the username
/// * `pass_env` - Environment variable name for the password
///
/// # Example
/// ```no_run
/// use datafusion::prelude::SessionContext;
/// use skardi::sources::hierarchy::HierarchyLevel;
/// use skardi::sources::providers::clickhouse::register_clickhouse_tables;
/// use std::collections::HashMap;
///
/// # async fn example() -> anyhow::Result<()> {
/// let mut ctx = SessionContext::new();
/// let options = HashMap::from([
///     ("table".to_string(), "events".to_string()),
///     ("database".to_string(), "analytics".to_string()),
/// ]);
/// register_clickhouse_tables(
///     &mut ctx,
///     "events",
///     "http://localhost:8123",
///     Some(&options),
///     HierarchyLevel::Table,
/// )
/// .await?;
/// # Ok(())
/// # }
/// ```
pub async fn register_clickhouse_tables(
    session_ctx: &mut SessionContext,
    name: &str,
    connection_string: &str,
    options: Option<&HashMap<String, String>>,
    hierarchy_level: HierarchyLevel,
) -> Result<()> {
    match hierarchy_level {
        HierarchyLevel::Catalog => {
            register_clickhouse_catalog(session_ctx, name, connection_string, options).await
        }
        HierarchyLevel::Table => {
            register_single_clickhouse_table(session_ctx, name, connection_string, options).await
        }
    }
}

/// Register one ClickHouse table under `name` in the default catalog.
async fn register_single_clickhouse_table(
    session_ctx: &mut SessionContext,
    name: &str,
    connection_string: &str,
    options: Option<&HashMap<String, String>>,
) -> Result<()> {
    let table_name = options
        .and_then(|opts| opts.get(OPT_TABLE))
        .ok_or_else(|| {
            anyhow!("ClickHouse data source '{name}' requires a '{OPT_TABLE}' option")
        })?;
    let database = options.and_then(|opts| opts.get(OPT_DATABASE));

    tracing::info!(
        "Registering ClickHouse table '{}' as '{}' against endpoint {} (read-only)",
        database
            .map(|db| format!("{db}.{table_name}"))
            .unwrap_or_else(|| table_name.clone()),
        name,
        connection_string
    );

    let params = parse_connection_params(connection_string, options)?;
    let label = SourceLabel::new(DataSourceType::Clickhouse, HierarchyLevel::Table, name);
    let pool = build_pool(label, params)
        .await
        .with_context(|| format!("Failed to create ClickHouse connection pool for '{name}'"))?;

    let table_reference = match database {
        Some(db) => TableReference::partial(db.as_str(), table_name.as_str()),
        None => TableReference::bare(table_name.as_str()),
    };

    let table_provider = build_clickhouse_table_provider(&pool, table_reference.clone()).await?;

    session_ctx
        .register_table(name, table_provider)
        .with_context(|| format!("Failed to register ClickHouse table '{name}' with DataFusion"))?;

    tracing::info!(
        "Successfully registered ClickHouse table '{}' as '{}' (read-only)",
        table_reference,
        name
    );

    Ok(())
}

/// Register an entire ClickHouse server as a named DataFusion catalog with one
/// schema per database.
async fn register_clickhouse_catalog(
    session_ctx: &mut SessionContext,
    catalog_name: &str,
    connection_string: &str,
    options: Option<&HashMap<String, String>>,
) -> Result<()> {
    tracing::info!(
        "Registering ClickHouse catalog '{}' against endpoint {} (read-only)",
        catalog_name,
        connection_string
    );

    let params = parse_connection_params(connection_string, options)?;
    let label = SourceLabel::new(
        DataSourceType::Clickhouse,
        HierarchyLevel::Catalog,
        catalog_name,
    );
    let pool = build_pool(label, params).await.with_context(|| {
        format!("Failed to create ClickHouse connection pool for catalog '{catalog_name}'")
    })?;

    let allowed_schemas = parse_allowed_schemas(options);
    let schema_tables = retry_with_timeout(label, "system.tables introspection", || async {
        list_clickhouse_tables(&pool, allowed_schemas.as_deref()).await
    })
    .await
    .with_context(|| {
        format!(
            "Failed to list ClickHouse tables for catalog-wide registration in source \
             '{catalog_name}'"
        )
    })?;

    if schema_tables.is_empty() {
        tracing::warn!(
            "No tables found in ClickHouse catalog for source '{}'",
            catalog_name
        );
    }

    let table_count = schema_tables.len();

    build_catalog(
        session_ctx,
        catalog_name,
        schema_tables,
        |schema, table_name| {
            let pool = Arc::clone(&pool);
            async move {
                let table_reference = TableReference::partial(schema.as_str(), table_name.as_str());
                build_clickhouse_table_provider(&pool, table_reference).await
            }
        },
    )
    .await
    .with_context(|| format!("Failed to build ClickHouse catalog '{catalog_name}'"))?;

    tracing::info!(
        "Registered ClickHouse catalog '{}' with {} table(s) (read-only)",
        catalog_name,
        table_count
    );

    Ok(())
}

/// Create the connection pool with per-attempt timeout and retries. The pool
/// constructor issues a `SELECT 1`, so an unreachable endpoint or bad
/// credentials fail here — at config load — rather than at first query.
async fn build_pool(
    label: SourceLabel<'_>,
    params: HashMap<String, SecretString>,
) -> Result<Arc<ClickHouseConnectionPool>> {
    let pool = retry_with_timeout(label, "pool creation", || async {
        ClickHouseConnectionPool::new(params.clone())
            .await
            .map_err(|e| anyhow!(e))
    })
    .await?;
    Ok(Arc::new(pool))
}

/// Build a read-only [`TableProvider`] for a single ClickHouse table. Schema is
/// inferred eagerly via `DESCRIBE TABLE`, so a missing table fails registration
/// with an actionable error instead of an opaque failure at query time.
async fn build_clickhouse_table_provider(
    pool: &Arc<ClickHouseConnectionPool>,
    table_reference: TableReference,
) -> Result<Arc<dyn TableProvider>> {
    let factory = ClickHouseTableFactory::new(Arc::clone(pool));
    let inner = factory
        .table_provider(table_reference.clone(), None)
        .await
        .map_err(|e| {
            anyhow!(
                "Failed to create ClickHouse table provider for '{table_reference}' \
                 (schema is fetched at registration time); check that the table exists \
                 and the configured user can read it: {e}"
            )
        })?;
    Ok(Arc::new(CountSafeClickHouseTable { inner }))
}

/// Thin wrapper working around an upstream `datafusion-table-providers` 0.10.1
/// bug shared with the Flight provider (see `CountSafeFlightTable` in
/// `influxdb.rs`): when DataFusion requests an **empty** projection (e.g.
/// `SELECT count(*)`), the inner ClickHouse table's unparsed SQL still selects
/// a column, so the physical plan advertises one field while the logical
/// schema has zero — and DataFusion aborts with "Physical input schema should
/// be the same as the one converted from logical input schema".
///
/// We intercept the empty-projection case: scan a single real column through
/// the inner table, then strip it back to zero columns with a
/// [`ProjectionExec`], which preserves the row count. All other projections
/// delegate straight to the inner table.
#[derive(Debug)]
struct CountSafeClickHouseTable {
    inner: Arc<dyn TableProvider>,
}

#[async_trait]
impl TableProvider for CountSafeClickHouseTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }

    fn table_type(&self) -> TableType {
        self.inner.table_type()
    }

    // Forward the remaining planning hooks so the wrapper is transparent to
    // the optimizer apart from the count(*) interception below.
    fn statistics(&self) -> Option<Statistics> {
        self.inner.statistics()
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> datafusion::common::Result<Vec<TableProviderFilterPushDown>> {
        self.inner.supports_filters_pushdown(filters)
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        match projection {
            // Empty projection (count(*) / EXISTS): fetch one column so the
            // unparsed ClickHouse SQL stays well-formed, then drop it again to
            // honour the requested zero-column output.
            Some(p) if p.is_empty() => {
                let single = vec![0usize];
                let plan = self
                    .inner
                    .scan(state, Some(&single), filters, limit)
                    .await?;
                let empty: Vec<(Arc<dyn PhysicalExpr>, String)> = Vec::new();
                Ok(Arc::new(ProjectionExec::try_new(empty, plan)?))
            }
            _ => self.inner.scan(state, projection, filters, limit).await,
        }
    }
}

/// List `(database, table)` pairs across the server via `system.tables`.
///
/// When `allowed_schemas` is `Some`, only those databases are introspected (a
/// database with no tables — or that doesn't exist — simply contributes
/// nothing). Otherwise every non-system database on the server is included.
async fn list_clickhouse_tables(
    pool: &Arc<ClickHouseConnectionPool>,
    allowed_schemas: Option<&[String]>,
) -> Result<Vec<(String, String)>> {
    let client = pool.client();

    let databases = match allowed_schemas {
        Some(allowed) => allowed.to_vec(),
        None => client
            .schemas()
            .await
            .map_err(|e| anyhow!("Failed to list ClickHouse databases: {e}"))?,
    };

    let mut schema_tables = Vec::new();
    for database in databases {
        let tables = client.tables(&database).await.map_err(|e| {
            anyhow!("Failed to list tables in ClickHouse database '{database}': {e}")
        })?;
        schema_tables.extend(tables.into_iter().map(|t| (database.clone(), t)));
    }

    Ok(schema_tables)
}

/// Parse a ClickHouse HTTP connection string plus Skardi options into the
/// parameter map expected by [`ClickHouseConnectionPool`].
///
/// The endpoint must be an `http://` or `https://` URL (ClickHouse's native
/// TCP port 9000 speaks a different protocol — this provider uses the HTTP
/// interface on port 8123). Credentials must come from `user_env` / `pass_env`
/// options; `user:pass@host` embedded in the URL is ignored by the pool, so a
/// warning is surfaced when detected.
fn parse_connection_params(
    connection_string: &str,
    options: Option<&HashMap<String, String>>,
) -> Result<HashMap<String, SecretString>> {
    let parsed = url::Url::parse(connection_string)
        .with_context(|| format!("Invalid ClickHouse connection string: {connection_string}"))?;

    match parsed.scheme() {
        "http" | "https" => {}
        other => {
            return Err(anyhow!(
                "ClickHouse connection string must use the http:// or https:// interface \
                 (got '{other}://'); the native TCP protocol is not supported"
            ));
        }
    }

    if !parsed.username().is_empty() || parsed.password().is_some() {
        tracing::warn!(
            "ClickHouse connection string contains embedded credentials; these are ignored. \
             Use '{OPT_USER_ENV}' and '{OPT_PASS_ENV}' options instead."
        );
    }

    let mut params: HashMap<String, SecretString> = HashMap::new();
    params.insert(
        "url".to_string(),
        SecretString::new(connection_string.to_string().into_boxed_str()),
    );

    if let Some(opts) = options {
        if let Some(database) = opts.get(OPT_DATABASE) {
            params.insert(
                "database".to_string(),
                SecretString::new(database.clone().into_boxed_str()),
            );
        }

        if let Some(user_env) = opts.get(OPT_USER_ENV) {
            let username = std::env::var(user_env).with_context(|| {
                format!("Environment variable '{user_env}' not found for ClickHouse user")
            })?;
            params.insert(
                "user".to_string(),
                SecretString::new(username.into_boxed_str()),
            );
        }

        if let Some(pass_env) = opts.get(OPT_PASS_ENV) {
            let password = std::env::var(pass_env).with_context(|| {
                format!("Environment variable '{pass_env}' not found for ClickHouse password")
            })?;
            params.insert(
                "password".to_string(),
                SecretString::new(password.into_boxed_str()),
            );
        }
    }

    Ok(params)
}

#[cfg(test)]
mod tests {
    use super::*;
    use secrecy::ExposeSecret;

    fn opts(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn parse_connection_params_url_passthrough() {
        let params = parse_connection_params("http://localhost:8123", None).unwrap();
        assert_eq!(
            params.get("url").unwrap().expose_secret(),
            "http://localhost:8123"
        );
        assert!(!params.contains_key("database"));
        assert!(!params.contains_key("user"));
        assert!(!params.contains_key("password"));
    }

    #[test]
    fn parse_connection_params_https_is_accepted() {
        let params = parse_connection_params("https://ch.example.com:8443", None).unwrap();
        assert_eq!(
            params.get("url").unwrap().expose_secret(),
            "https://ch.example.com:8443"
        );
    }

    #[test]
    fn parse_connection_params_database_option() {
        let params = parse_connection_params(
            "http://localhost:8123",
            Some(&opts(&[("database", "analytics")])),
        )
        .unwrap();
        assert_eq!(params.get("database").unwrap().expose_secret(), "analytics");
    }

    #[test]
    fn parse_connection_params_rejects_native_protocol() {
        let err = parse_connection_params("tcp://localhost:9000", None).unwrap_err();
        assert!(err.to_string().contains("http:// or https://"), "got {err}");
    }

    #[test]
    fn parse_connection_params_rejects_invalid_url() {
        let err = parse_connection_params("not-a-valid-url", None).unwrap_err();
        assert!(
            err.to_string()
                .contains("Invalid ClickHouse connection string"),
            "got {err}"
        );
    }

    #[test]
    fn parse_connection_params_env_credentials() {
        let user_var = "SKARDI_TEST_CLICKHOUSE_USER_OK";
        let pass_var = "SKARDI_TEST_CLICKHOUSE_PASS_OK";
        unsafe {
            std::env::set_var(user_var, "testuser");
            std::env::set_var(pass_var, "testpass");
        }
        let params = parse_connection_params(
            "http://localhost:8123",
            Some(&opts(&[("user_env", user_var), ("pass_env", pass_var)])),
        )
        .unwrap();
        unsafe {
            std::env::remove_var(user_var);
            std::env::remove_var(pass_var);
        }
        assert_eq!(params.get("user").unwrap().expose_secret(), "testuser");
        assert_eq!(params.get("password").unwrap().expose_secret(), "testpass");
    }

    #[test]
    fn parse_connection_params_missing_user_env_is_an_error() {
        let err = parse_connection_params(
            "http://localhost:8123",
            Some(&opts(&[(
                "user_env",
                "SKARDI_TEST_CLICKHOUSE_USER_DEFINITELY_UNSET",
            )])),
        )
        .unwrap_err();
        assert!(err.to_string().contains("not found"), "got {err}");
    }

    #[test]
    fn parse_connection_params_missing_pass_env_is_an_error() {
        let err = parse_connection_params(
            "http://localhost:8123",
            Some(&opts(&[(
                "pass_env",
                "SKARDI_TEST_CLICKHOUSE_PASS_DEFINITELY_UNSET",
            )])),
        )
        .unwrap_err();
        assert!(err.to_string().contains("not found"), "got {err}");
    }

    #[test]
    fn parse_connection_params_embedded_credentials_are_ignored() {
        // Embedded creds only trigger a warning; they must not leak into the
        // param map where they'd silently override the env-based options.
        let params = parse_connection_params("http://user:pass@localhost:8123", None).unwrap();
        assert!(!params.contains_key("user"));
        assert!(!params.contains_key("password"));
    }

    #[tokio::test]
    async fn register_without_table_option_errors_before_connecting() {
        // The option-validation error must fire before any network call, so
        // this is safe to run offline (the endpoint is deliberately unroutable).
        let mut ctx = SessionContext::new();
        let err = register_clickhouse_tables(
            &mut ctx,
            "events",
            "http://127.0.0.1:1",
            None,
            HierarchyLevel::Table,
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("requires a 'table'"), "got {err}");
    }

    #[tokio::test]
    async fn register_with_bad_scheme_errors_before_connecting() {
        let mut ctx = SessionContext::new();
        let options = opts(&[("table", "events")]);
        let err = register_clickhouse_tables(
            &mut ctx,
            "events",
            "clickhouse://127.0.0.1:9000",
            Some(&options),
            HierarchyLevel::Table,
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("http:// or https://"), "got {err}");
    }

    // ─── Integration tests (need a live ClickHouse endpoint) ────────────
    //
    // Gated with `#[ignore]`; CI runs them via `cargo nextest -- --ignored`
    // after starting a ClickHouse service container and seeding the `mydb`
    // database (see .github/workflows/ci.yml and docs/clickhouse/README.md).
    // Endpoint, database, and credentials are read from env so the same tests
    // run locally (against a default-user Docker container) and in CI.

    fn clickhouse_url() -> String {
        std::env::var("CLICKHOUSE_URL").unwrap_or_else(|_| "http://127.0.0.1:8123".to_string())
    }

    fn clickhouse_database() -> String {
        std::env::var("CLICKHOUSE_DATABASE").unwrap_or_else(|_| "mydb".to_string())
    }

    /// Base options for the CI/local fixture: the seeded database plus
    /// env-based credentials when `CLICKHOUSE_USER` / `CLICKHOUSE_PASSWORD`
    /// are set (CI); a plain local container uses the `default` user with no
    /// credentials, so the options are simply omitted there.
    fn ci_options() -> HashMap<String, String> {
        let mut options = opts(&[("database", clickhouse_database().as_str())]);
        if std::env::var("CLICKHOUSE_USER").is_ok() {
            options.insert("user_env".to_string(), "CLICKHOUSE_USER".to_string());
        }
        if std::env::var("CLICKHOUSE_PASSWORD").is_ok() {
            options.insert("pass_env".to_string(), "CLICKHOUSE_PASSWORD".to_string());
        }
        options
    }

    async fn register_ci_table(ctx: &mut SessionContext, name: &str, table: &str) {
        let mut options = ci_options();
        options.insert("table".to_string(), table.to_string());
        register_clickhouse_tables(
            ctx,
            name,
            &clickhouse_url(),
            Some(&options),
            HierarchyLevel::Table,
        )
        .await
        .unwrap_or_else(|e| panic!("register {name} failed: {e}"));
    }

    fn total_rows(batches: &[datafusion::arrow::record_batch::RecordBatch]) -> usize {
        batches.iter().map(|b| b.num_rows()).sum()
    }

    async fn collect(
        ctx: &SessionContext,
        sql: &str,
    ) -> Vec<datafusion::arrow::record_batch::RecordBatch> {
        ctx.sql(sql)
            .await
            .unwrap_or_else(|e| panic!("plan {sql}: {e}"))
            .collect()
            .await
            .unwrap_or_else(|e| panic!("collect {sql}: {e}"))
    }

    #[tokio::test]
    #[ignore]
    async fn integration_register_table_and_scan() {
        let mut ctx = SessionContext::new();
        register_ci_table(&mut ctx, "users", "users").await;

        let batches = collect(&ctx, "SELECT id, name, email FROM users ORDER BY id").await;
        assert_eq!(total_rows(&batches), 3, "expected the 3 seeded users");
        let names = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringArray>()
            .expect("name is Utf8");
        assert_eq!(names.value(0), "Alice Smith");
    }

    #[tokio::test]
    #[ignore]
    async fn integration_filter_pushdown() {
        let mut ctx = SessionContext::new();
        register_ci_table(&mut ctx, "users", "users").await;

        let batches = collect(&ctx, "SELECT name FROM users WHERE id = 2").await;
        assert_eq!(total_rows(&batches), 1);
        let names = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringArray>()
            .expect("name is Utf8");
        assert_eq!(names.value(0), "Bob Johnson");
    }

    #[tokio::test]
    #[ignore]
    async fn integration_count_star_empty_projection() {
        // count(*) requests an empty projection — a shape that has broken
        // other providers (see influxdb.rs); assert it returns the row count.
        let mut ctx = SessionContext::new();
        register_ci_table(&mut ctx, "users", "users").await;

        let batches = collect(&ctx, "SELECT count(*) AS n FROM users").await;
        let n = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Int64Array>()
            .expect("count is Int64")
            .value(0);
        assert_eq!(n, 3);
    }

    #[tokio::test]
    #[ignore]
    async fn integration_null_bearing_rows() {
        // `products.category` is Nullable(String) and the fixture has exactly
        // one row with a NULL category. Assert both NULL and non-NULL cells
        // survive the trip into Arrow.
        let mut ctx = SessionContext::new();
        register_ci_table(&mut ctx, "products", "products").await;

        let batches = collect(
            &ctx,
            "SELECT count(*) AS n FROM products WHERE category IS NULL",
        )
        .await;
        let nulls = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Int64Array>()
            .expect("count is Int64")
            .value(0);
        assert_eq!(nulls, 1, "expected exactly one NULL-category product");

        let batches = collect(
            &ctx,
            "SELECT count(*) AS n FROM products WHERE category IS NOT NULL",
        )
        .await;
        let non_nulls = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Int64Array>()
            .expect("count is Int64")
            .value(0);
        assert_eq!(non_nulls, 4);
    }

    #[tokio::test]
    #[ignore]
    async fn integration_empty_table_schema_inference() {
        // Schema inference must come from DESCRIBE, not from sampled rows —
        // an empty table has to register and scan cleanly (regression case:
        // several providers have shipped with panics here).
        let mut ctx = SessionContext::new();
        register_ci_table(&mut ctx, "empty_metrics", "empty_metrics").await;

        let df = ctx
            .sql("SELECT ts, value FROM empty_metrics")
            .await
            .expect("plan");
        let schema = df.schema().clone();
        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(0).name(), "ts");
        assert_eq!(schema.field(1).name(), "value");

        let batches = df.collect().await.expect("collect");
        assert_eq!(total_rows(&batches), 0);
    }

    #[tokio::test]
    #[ignore]
    async fn integration_aggregation_over_numeric_types() {
        // ClickHouse Float64 / UInt32 must coerce into Arrow numerics that
        // DataFusion can aggregate. Assert an actual value, not just success.
        let mut ctx = SessionContext::new();
        register_ci_table(&mut ctx, "products", "products").await;

        let batches = collect(
            &ctx,
            "SELECT min(price) AS lo, max(price) AS hi FROM products",
        )
        .await;
        assert_eq!(total_rows(&batches), 1);
        let lo = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Float64Array>()
            .expect("min(price) is Float64")
            .value(0);
        let hi = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Float64Array>()
            .expect("max(price) is Float64")
            .value(0);
        assert_eq!(lo, 29.99);
        assert_eq!(hi, 999.99);
    }

    #[tokio::test]
    #[ignore]
    async fn integration_catalog_mode_registers_all_tables() {
        let mut ctx = SessionContext::new();
        let mut options = ci_options();
        // Catalog mode must not carry per-table options; keep database out too
        // so the pool stays on the server default and references are qualified.
        options.remove("database");
        options.insert("allowed_schemas".to_string(), clickhouse_database());
        register_clickhouse_tables(
            &mut ctx,
            "ch",
            &clickhouse_url(),
            Some(&options),
            HierarchyLevel::Catalog,
        )
        .await
        .expect("register catalog");

        let db = clickhouse_database();
        let batches = collect(&ctx, &format!("SELECT count(*) AS n FROM ch.{db}.users")).await;
        let n = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Int64Array>()
            .expect("count is Int64")
            .value(0);
        assert_eq!(n, 3);

        let batches = collect(
            &ctx,
            &format!("SELECT count(*) AS n FROM ch.{db}.products WHERE in_stock"),
        )
        .await;
        let n = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Int64Array>()
            .expect("count is Int64")
            .value(0);
        assert_eq!(n, 4, "expected 4 in-stock products");
    }
}
