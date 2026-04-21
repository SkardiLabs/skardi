//! `skardi job ...` subcommands — thin HTTP clients against the server's
//! `/jobs/*` endpoints.
//!
//! Jobs run *only* inside `skardi-server`; unlike `skardi run` (pipelines,
//! in-process), there is no CLI fallback. Every command here POSTs/GETs
//! the server and pretty-prints the JSON response.

use anyhow::{Context, Result};
use clap::Subcommand;
use datafusion::common::ScalarValue;
use serde::Deserialize;
use serde_json::{Map, Value};

use crate::pipeline::parse_param_flag;

#[derive(Subcommand)]
pub enum JobCmd {
    /// Submit a new run of the named job. Returns the run_id immediately;
    /// poll with `skardi job status <run_id>` to follow progress.
    Run {
        /// Job name (from `metadata.name` in the job YAML)
        job: String,
        /// Bind a parameter: NAME=VALUE or NAME:TYPE=VALUE (types: str,
        /// int, float, bool). Same shape as `skardi run` --param.
        #[arg(short = 'p', long = "param", value_name = "NAME=VALUE")]
        params: Vec<String>,
        /// Server base URL (default: $SKARDI_SERVER_URL or http://127.0.0.1:8080)
        #[arg(long = "server", value_name = "URL")]
        server: Option<String>,
    },
    /// Print the current status of one run.
    Status {
        /// Run id returned by `skardi job run`.
        run_id: String,
        #[arg(long = "server", value_name = "URL")]
        server: Option<String>,
    },
    /// List recent runs. Pass --job to filter by job name.
    List {
        #[arg(long)]
        job: Option<String>,
        #[arg(long, default_value = "20")]
        limit: usize,
        #[arg(long = "server", value_name = "URL")]
        server: Option<String>,
    },
    /// Request cancellation of an in-progress run. The server sets a flag
    /// that the background task checks before committing; runs that have
    /// already committed are reported as `cancelled: false`.
    Cancel {
        run_id: String,
        #[arg(long = "server", value_name = "URL")]
        server: Option<String>,
    },
    /// List every job the server knows about and its destination.
    Show {
        #[arg(long = "server", value_name = "URL")]
        server: Option<String>,
    },
}

fn server_base_url(override_url: Option<&str>) -> String {
    override_url
        .map(|s| s.to_string())
        .or_else(|| std::env::var("SKARDI_SERVER_URL").ok())
        .unwrap_or_else(|| "http://127.0.0.1:8080".to_string())
}

/// Convert CLI `--param NAME=VALUE` tokens into the JSON object the
/// server expects. Uses the same parse path as `skardi run` so typing
/// rules stay consistent.
fn params_to_json(raw: &[String]) -> Result<Map<String, Value>> {
    let mut out = Map::new();
    for s in raw {
        let (name, value) = parse_param_flag(s)?;
        out.insert(name, scalar_to_json(&value)?);
    }
    Ok(out)
}

fn scalar_to_json(v: &ScalarValue) -> Result<Value> {
    Ok(match v {
        ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => Value::String(s.clone()),
        ScalarValue::Utf8(None) | ScalarValue::LargeUtf8(None) | ScalarValue::Null => Value::Null,
        ScalarValue::Int64(Some(i)) => Value::Number((*i).into()),
        ScalarValue::Int32(Some(i)) => Value::Number((*i as i64).into()),
        ScalarValue::Float64(Some(f)) => serde_json::Number::from_f64(*f)
            .map(Value::Number)
            .ok_or_else(|| anyhow::anyhow!("Non-finite float cannot be sent as JSON: {f}"))?,
        ScalarValue::Float32(Some(f)) => serde_json::Number::from_f64(*f as f64)
            .map(Value::Number)
            .ok_or_else(|| anyhow::anyhow!("Non-finite float cannot be sent as JSON: {f}"))?,
        ScalarValue::Boolean(Some(b)) => Value::Bool(*b),
        other => anyhow::bail!("Unsupported parameter scalar: {other:?}"),
    })
}

#[derive(Debug, Deserialize)]
struct RunIdResponse {
    run_id: String,
    #[serde(default)]
    status: Option<String>,
}

fn client() -> Result<reqwest::Client> {
    reqwest::Client::builder()
        .build()
        .context("Failed to construct HTTP client")
}

async fn get_json(url: &str) -> Result<Value> {
    let resp = client()?
        .get(url)
        .send()
        .await
        .with_context(|| format!("Failed to GET {url}"))?;
    let status = resp.status();
    let body = resp
        .text()
        .await
        .with_context(|| format!("Failed to read response body from {url}"))?;
    if !status.is_success() {
        anyhow::bail!("Server returned {}: {}", status, body);
    }
    serde_json::from_str(&body).with_context(|| format!("Failed to parse JSON from {url}: {body}"))
}

async fn post_json(url: &str, body: Value) -> Result<Value> {
    let resp = client()?
        .post(url)
        .json(&body)
        .send()
        .await
        .with_context(|| format!("Failed to POST {url}"))?;
    let status = resp.status();
    let body = resp
        .text()
        .await
        .with_context(|| format!("Failed to read response body from {url}"))?;
    if !status.is_success() {
        anyhow::bail!("Server returned {}: {}", status, body);
    }
    serde_json::from_str(&body).with_context(|| format!("Failed to parse JSON from {url}: {body}"))
}

pub async fn handle_job_cmd(cmd: JobCmd) -> Result<()> {
    match cmd {
        JobCmd::Run {
            job,
            params,
            server,
        } => {
            let base = server_base_url(server.as_deref());
            let body = Value::Object(params_to_json(&params)?);
            let url = format!("{base}/jobs/{job}/run");
            let resp: RunIdResponse = serde_json::from_value(post_json(&url, body).await?)?;
            println!(
                "submitted: {} ({})",
                resp.run_id,
                resp.status.as_deref().unwrap_or("pending")
            );
        }
        JobCmd::Status { run_id, server } => {
            let base = server_base_url(server.as_deref());
            let url = format!("{base}/jobs/runs/{run_id}");
            let resp = get_json(&url).await?;
            println!("{}", serde_json::to_string_pretty(&resp)?);
        }
        JobCmd::List { job, limit, server } => {
            let base = server_base_url(server.as_deref());
            let mut url = format!("{base}/jobs/runs?limit={limit}");
            if let Some(j) = job {
                url.push_str(&format!("&job={}", urlencode(&j)));
            }
            let resp = get_json(&url).await?;
            print_run_list(&resp);
        }
        JobCmd::Cancel { run_id, server } => {
            let base = server_base_url(server.as_deref());
            let url = format!("{base}/jobs/runs/{run_id}/cancel");
            let resp = post_json(&url, Value::Object(Map::new())).await?;
            println!("{}", serde_json::to_string_pretty(&resp)?);
        }
        JobCmd::Show { server } => {
            let base = server_base_url(server.as_deref());
            let url = format!("{base}/jobs");
            let resp = get_json(&url).await?;
            println!("{}", serde_json::to_string_pretty(&resp)?);
        }
    }
    Ok(())
}

fn urlencode(s: &str) -> String {
    // Minimal url-encoding: only escape the characters most likely to
    // appear in a job name (`/`, `&`, `?`). Good enough for a CLI.
    s.replace('&', "%26")
        .replace('?', "%3F")
        .replace('/', "%2F")
}

fn print_run_list(resp: &Value) {
    let Some(runs) = resp.get("runs").and_then(|v| v.as_array()) else {
        println!("{}", serde_json::to_string_pretty(resp).unwrap_or_default());
        return;
    };
    if runs.is_empty() {
        println!("(no runs)");
        return;
    }
    for run in runs {
        let id = run.get("run_id").and_then(|v| v.as_str()).unwrap_or("?");
        let job = run.get("job").and_then(|v| v.as_str()).unwrap_or("?");
        let status = run.get("status").and_then(|v| v.as_str()).unwrap_or("?");
        let created = run
            .get("created_at")
            .and_then(|v| v.as_str())
            .unwrap_or("?");
        let rows = run
            .get("rows_written")
            .and_then(|v| v.as_u64())
            .map(|n| n.to_string())
            .unwrap_or_else(|| "-".to_string());
        println!("{id}  {status:<10}  {job:<25}  rows={rows:<8}  created_at={created}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn params_to_json_handles_types() {
        let params = vec![
            "s=hello".to_string(),
            "n=42".to_string(),
            "f=0.5".to_string(),
            "b=true".to_string(),
        ];
        let out = params_to_json(&params).unwrap();
        assert_eq!(out.get("s").unwrap(), &Value::String("hello".into()));
        assert_eq!(
            out.get("n").unwrap(),
            &Value::Number(serde_json::Number::from(42))
        );
        assert_eq!(out.get("b").unwrap(), &Value::Bool(true));
        assert_eq!(out.get("f").unwrap().as_f64().unwrap(), 0.5);
    }

    #[test]
    fn server_url_precedence() {
        // Explicit override beats env and default.
        assert_eq!(
            server_base_url(Some("http://host.test:1234")),
            "http://host.test:1234"
        );
    }
}
