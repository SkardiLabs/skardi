//! `skardi job ...` — thin HTTP client for the server's `/jobs/*` endpoints.
//!
//! Jobs run *only* inside `skardi-server`; unlike `skardi run` (pipelines),
//! there is no CLI fallback. Every command here `GET`s/`POST`s the server via
//! the shared [`ApiClient`] and pretty-prints (or otherwise renders) the JSON
//! response.

use crate::client::ApiClient;
use crate::params::build_body;
use anyhow::Result;
use clap::Subcommand;
use serde::Deserialize;
use serde_json::{Map, Value};

/// `job` subcommands.
#[derive(Subcommand, Debug)]
pub enum JobCmd {
    /// Submit a new run of the named job. Returns the run_id immediately;
    /// poll with `skardi job status <run_id>` to follow progress.
    Run {
        /// Job name (from `metadata.name` in the job YAML)
        job: String,
        /// NAME=VALUE parameter (repeatable); values are parsed as JSON
        /// first (numbers, booleans, arrays, null, quoted strings) and fall
        /// back to a plain string otherwise.
        #[arg(short = 'p', long = "param", value_name = "NAME=VALUE")]
        params: Vec<String>,
    },
    /// Print the current status of one run.
    Status {
        /// Run id returned by `skardi job run`.
        run_id: String,
    },
    /// List recent runs. Pass --job to filter by job name.
    List {
        #[arg(long)]
        job: Option<String>,
        #[arg(long, default_value = "20")]
        limit: usize,
    },
    /// Request cancellation of an in-progress run. The server sets a flag
    /// that the background task checks before committing; runs that have
    /// already committed are reported as `cancelled: false`.
    Cancel {
        /// Run id returned by `skardi job run`.
        run_id: String,
    },
    /// List every job the server knows about and its destination.
    Show,
}

/// The server's `{run_id, status}` response to `POST /jobs/<job>/run`.
#[derive(Debug, Deserialize)]
struct RunIdResponse {
    run_id: String,
    #[serde(default)]
    status: Option<String>,
}

/// Run `skardi job <cmd>` against `client`.
pub async fn run(client: &ApiClient, cmd: JobCmd) -> Result<()> {
    match cmd {
        JobCmd::Run { job, params } => {
            let body = build_body(None, &params)?;
            let path = format!("/jobs/{job}/run");
            let response = client.post(&path, &Value::Object(body)).await?;
            let resp: RunIdResponse = serde_json::from_value(response)?;
            println!(
                "submitted: {} ({})",
                resp.run_id,
                resp.status.as_deref().unwrap_or("pending")
            );
        }
        JobCmd::Status { run_id } => {
            let path = format!("/jobs/runs/{run_id}");
            let response = client.get(&path).await?;
            println!("{}", serde_json::to_string_pretty(&response)?);
        }
        JobCmd::List { job, limit } => {
            let mut path = format!("/jobs/runs?limit={limit}");
            if let Some(job) = job {
                path.push_str(&format!("&job={}", urlencode(&job)));
            }
            let response = client.get(&path).await?;
            print_run_list(&response);
        }
        JobCmd::Cancel { run_id } => {
            let path = format!("/jobs/runs/{run_id}/cancel");
            let response = client.post(&path, &Value::Object(Map::new())).await?;
            println!("{}", serde_json::to_string_pretty(&response)?);
        }
        JobCmd::Show => {
            let response = client.get("/jobs").await?;
            println!("{}", serde_json::to_string_pretty(&response)?);
        }
    }

    Ok(())
}

/// Minimal url-encoding: only escape the characters most likely to appear in
/// a job name (`/`, `&`, `?`). Good enough for a CLI.
fn urlencode(s: &str) -> String {
    s.replace('&', "%26")
        .replace('?', "%3F")
        .replace('/', "%2F")
}

/// Render `GET /jobs/runs`'s `{"runs": [...]}` response as a columnar list;
/// `(no runs)` when empty, and a pretty-JSON fallback when the response
/// doesn't have the expected shape (e.g. missing/non-array `runs`).
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
    use super::{JobCmd, run};
    use crate::client::ApiClient;
    use crate::config::ClientConfig;
    use serde_json::json;
    use wiremock::matchers::{body_json, method, path, query_param};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn test_config(server: &str) -> ClientConfig {
        ClientConfig {
            server: server.to_string(),
            token: None,
        }
    }

    // -- 1. `job run` posts typed params; string stays a string, number
    //       stays a number ---------------------------------------------

    #[tokio::test]
    async fn run_posts_typed_params_and_succeeds() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/jobs/nightly_sync/run"))
            .and(body_json(json!({"day": "2026-07-23", "batch": 500})))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(json!({"run_id": "r-1", "status": "pending"})),
            )
            .expect(1)
            .mount(&server)
            .await;

        let client = ApiClient::new(&test_config(&server.uri())).unwrap();
        let result = run(
            &client,
            JobCmd::Run {
                job: "nightly_sync".to_string(),
                params: vec!["day=2026-07-23".to_string(), "batch=500".to_string()],
            },
        )
        .await;

        assert!(result.is_ok(), "expected Ok, got {result:?}");
    }

    // -- 2. `job list` with a filter sends both limit and job query params --

    #[tokio::test]
    async fn list_with_filter_sends_limit_and_job_query_params() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/jobs/runs"))
            .and(query_param("limit", "5"))
            .and(query_param("job", "nightly_sync"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"runs": []})))
            .expect(1)
            .mount(&server)
            .await;

        let client = ApiClient::new(&test_config(&server.uri())).unwrap();
        let result = run(
            &client,
            JobCmd::List {
                job: Some("nightly_sync".to_string()),
                limit: 5,
            },
        )
        .await;

        assert!(result.is_ok(), "expected Ok, got {result:?}");
    }

    // -- 3. `job status` GETs and `job cancel` POSTs the run_id endpoints --

    #[tokio::test]
    async fn status_gets_run_endpoint() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/jobs/runs/r-1"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(json!({"run_id": "r-1", "status": "succeeded"})),
            )
            .expect(1)
            .mount(&server)
            .await;

        let client = ApiClient::new(&test_config(&server.uri())).unwrap();
        let result = run(
            &client,
            JobCmd::Status {
                run_id: "r-1".to_string(),
            },
        )
        .await;

        assert!(result.is_ok(), "expected Ok, got {result:?}");
    }

    #[tokio::test]
    async fn cancel_posts_cancel_endpoint() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/jobs/runs/r-1/cancel"))
            .and(body_json(json!({})))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(json!({"run_id": "r-1", "cancelled": true})),
            )
            .expect(1)
            .mount(&server)
            .await;

        let client = ApiClient::new(&test_config(&server.uri())).unwrap();
        let result = run(
            &client,
            JobCmd::Cancel {
                run_id: "r-1".to_string(),
            },
        )
        .await;

        assert!(result.is_ok(), "expected Ok, got {result:?}");
    }

    // -- 4. `job show` GETs /jobs -----------------------------------------

    #[tokio::test]
    async fn show_gets_jobs_endpoint() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/jobs"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(json!([{"name": "nightly_sync"}])),
            )
            .expect(1)
            .mount(&server)
            .await;

        let client = ApiClient::new(&test_config(&server.uri())).unwrap();
        let result = run(&client, JobCmd::Show).await;

        assert!(result.is_ok(), "expected Ok, got {result:?}");
    }
}
