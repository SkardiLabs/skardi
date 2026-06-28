//! Integration test for the `skardi query` CLI against an InfluxDB 3 source.
//!
//! Exercises the CLI's `"influxdb"` data-source dispatch arm end-to-end by
//! running the compiled binary against a live InfluxDB 3 endpoint.
//!
//! Gated with `#[ignore]`; CI runs it via `cargo nextest -- --ignored` after
//! starting an InfluxDB 3 Core container and seeding the `metrics` database
//! (see `.github/workflows/ci.yml` and `docs/influxdb/README.md`). Locally:
//!
//! ```bash
//! cargo test -p skardi-cli --test influxdb_cli -- --ignored
//! ```

use std::io::Write;
use std::process::Command;

fn influx_url() -> String {
    std::env::var("INFLUXDB_URL").unwrap_or_else(|_| "http://127.0.0.1:8181".to_string())
}

fn influx_database() -> String {
    std::env::var("INFLUXDB_DATABASE").unwrap_or_else(|_| "metrics".to_string())
}

#[test]
#[ignore]
fn cli_query_against_influxdb_source() {
    let tmp = tempfile::TempDir::new().unwrap();
    let ctx_path = tmp.path().join("ctx_influxdb.yaml");
    let ctx = format!(
        r#"kind: context
metadata:
  name: ctx_influxdb_cli_test
  version: 1.0.0
spec:
  data_sources:
    - name: "cpu"
      type: "influxdb"
      connection_string: "{url}"
      options:
        database: "{db}"
        measurement: "cpu"
"#,
        url = influx_url(),
        db = influx_database(),
    );
    std::fs::File::create(&ctx_path)
        .unwrap()
        .write_all(ctx.as_bytes())
        .unwrap();

    let output = Command::new(env!("CARGO_BIN_EXE_skardi"))
        .arg("query")
        .arg("--ctx")
        .arg(&ctx_path)
        .arg("--sql")
        .arg("SELECT count(*) AS n FROM cpu")
        .output()
        .expect("run skardi query");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "CLI query failed.\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );
    // The aggregate column name should appear in the rendered table output.
    assert!(
        stdout.contains('n'),
        "expected the count column in output, got:\n{stdout}"
    );
}
