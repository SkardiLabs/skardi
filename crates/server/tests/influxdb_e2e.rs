//! Integration tests for the InfluxDB 3 provider's wiring into the server's
//! data-source registration dispatch (`register_data_sources` →
//! `register_data_source` → `register_influxdb_tables`).
//!
//! Gated with `#[ignore]`; CI runs them via `cargo nextest -- --ignored`
//! after starting an InfluxDB 3 Core container and seeding the `metrics`
//! database (see `.github/workflows/ci.yml` and `docs/influxdb/README.md`).
//! Locally:
//!
//! ```bash
//! # start + seed InfluxDB per docs/influxdb/README.md, then:
//! cargo test -p skardi-server --test influxdb_e2e -- --ignored
//! ```

use std::collections::HashMap;
use std::path::PathBuf;

use datafusion::prelude::SessionContext;
use skardi_server::config::{AccessMode, DataSource, DataSourceType, register_data_sources};

fn influx_url() -> String {
    std::env::var("INFLUXDB_URL").unwrap_or_else(|_| "http://127.0.0.1:8181".to_string())
}

fn influx_database() -> String {
    std::env::var("INFLUXDB_DATABASE").unwrap_or_else(|_| "metrics".to_string())
}

/// Build an InfluxDB `DataSource` the same way the context loader would from
/// `ctx_influxdb_demo.yaml`, so the registration dispatch arm is exercised.
fn influx_source(name: &str, measurement: &str) -> DataSource {
    let mut options = HashMap::new();
    options.insert("database".to_string(), influx_database());
    options.insert("measurement".to_string(), measurement.to_string());
    DataSource {
        name: name.to_string(),
        source_type: DataSourceType::Influxdb,
        path: PathBuf::new(),
        connection_string: Some(influx_url()),
        schema: None,
        options: Some(options),
        hierarchy_level: Default::default(),
        access_mode: AccessMode::default(),
        enable_cache: false,
        description: None,
        open_connector: None,
    }
}

#[tokio::test]
#[ignore]
async fn influxdb_source_registers_and_queries_through_config_dispatch() {
    let mut ctx = SessionContext::new();
    let sources = vec![influx_source("cpu", "cpu"), influx_source("mem", "mem")];

    register_data_sources(&mut ctx, &sources)
        .await
        .expect("register InfluxDB data sources via config dispatch");

    let batches = ctx
        .sql("SELECT count(*) AS n FROM cpu")
        .await
        .expect("plan query")
        .collect()
        .await
        .expect("collect");
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 1, "count(*) returns a single row");

    // Both measurements registered as independent tables.
    let mem = ctx
        .sql("SELECT host, used_percent FROM mem")
        .await
        .expect("plan mem query")
        .collect()
        .await
        .expect("collect mem");
    let mem_rows: usize = mem.iter().map(|b| b.num_rows()).sum();
    assert!(
        mem_rows >= 3,
        "expected at least 3 mem rows, got {mem_rows}"
    );
}
