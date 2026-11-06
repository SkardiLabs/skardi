//! `skardi pipeline list|show` — list pipelines or show one pipeline's
//! definition via `GET /pipelines` / `GET /pipeline/<name>`, and pretty-print
//! the server's JSON response.

use crate::client::{ApiClient, ApiError, encode_component};
use anyhow::{Result, anyhow};
use clap::Subcommand;

/// `pipeline` subcommands: list all pipelines, or show one by name.
#[derive(Subcommand, Debug)]
pub enum PipelineCmd {
    /// List all pipelines known to the server.
    List,

    /// Show one pipeline's definition.
    Show {
        /// pipeline name
        name: String,
    },
}

/// Message for the definitive "no pipeline surface" outcome: `GET
/// /pipelines` itself 404s, so the server does not implement the pipeline
/// API at all — as opposed to serving pipelines but not knowing this
/// particular one. Deliberately silent on what kind of server this is: the
/// client can only observe that this deployment has no pipeline routes.
pub(crate) const NO_PIPELINE_SURFACE: &str = "this server does not serve pipelines — it looks like an older or differently configured deployment with no pipeline API";

/// Probe whether the server serves pipelines at all, via `GET /pipelines`.
///
/// Used to tell "this pipeline doesn't exist" apart from "this server
/// doesn't serve pipelines" when a pipeline-shaped 404 alone doesn't say
/// which. Only a 404 on the probe itself counts as the definitive "no" —
/// any other outcome (success, or some other error) can't rule out that
/// pipelines are served, so callers fall back to the ordinary not-found
/// message rather than asserting more than the probe supports.
pub(crate) async fn serves_pipelines(client: &ApiClient) -> bool {
    !matches!(
        client.get("/pipelines").await,
        Err(ApiError::Http { status: 404, .. })
    )
}

/// Run `skardi pipeline <cmd>`: `GET /pipelines` for `List`, or
/// `GET /pipeline/<name>` for `Show`, and pretty-print the response.
///
/// A 404 is not passed through as a raw HTTP error: for `List`, hitting
/// `/pipelines` itself 404ing is the definitive signal that this server has
/// no pipeline surface. For `Show`, a 404 on `/pipeline/<name>` is
/// ambiguous between that and "this pipeline doesn't exist", so it probes
/// `/pipelines` (see [`serves_pipelines`]) to tell them apart.
pub async fn run(client: &ApiClient, cmd: PipelineCmd) -> Result<()> {
    let path = match &cmd {
        PipelineCmd::List => "/pipelines".to_string(),
        PipelineCmd::Show { name } => format!("/pipeline/{}", encode_component(name)),
    };

    let response = match client.get(&path).await {
        Ok(response) => response,
        Err(ApiError::Http { status: 404, .. }) => {
            return Err(match &cmd {
                PipelineCmd::List => anyhow!(NO_PIPELINE_SURFACE),
                PipelineCmd::Show { name } => {
                    if serves_pipelines(client).await {
                        anyhow!("pipeline '{name}' not found — try 'skardi pipeline list'")
                    } else {
                        anyhow!(NO_PIPELINE_SURFACE)
                    }
                }
            });
        }
        Err(err) => return Err(err.into()),
    };

    let pretty = serde_json::to_string_pretty(&response)?;
    println!("{pretty}");

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{PipelineCmd, run};
    use crate::client::ApiClient;
    use crate::config::ClientConfig;
    use serde_json::json;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn test_config(server: &str) -> ClientConfig {
        ClientConfig {
            server: server.to_string(),
            token: None,
            context: None,
        }
    }

    #[tokio::test]
    async fn list_hits_pipelines_endpoint() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/pipelines"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(json!([{"name": "daily_report"}])),
            )
            .expect(1)
            .mount(&server)
            .await;

        let client = ApiClient::new(&test_config(&server.uri())).unwrap();
        let result = run(&client, PipelineCmd::List).await;

        assert!(result.is_ok(), "expected Ok, got {result:?}");
    }

    #[tokio::test]
    async fn show_hits_pipeline_name_endpoint() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/pipeline/daily_report"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"name": "daily_report"})))
            .expect(1)
            .mount(&server)
            .await;

        let client = ApiClient::new(&test_config(&server.uri())).unwrap();
        let result = run(
            &client,
            PipelineCmd::Show {
                name: "daily_report".to_string(),
            },
        )
        .await;

        assert!(result.is_ok(), "expected Ok, got {result:?}");
    }

    // -- list: a 404 on /pipelines is the definitive answer --------------

    #[tokio::test]
    async fn list_404_yields_no_pipeline_surface_message() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/pipelines"))
            .respond_with(ResponseTemplate::new(404).set_body_json(json!({
                "success": false,
                "error": "not found",
                "error_type": "not_found",
                "details": null,
                "timestamp": "2026-07-23T00:00:00Z",
            })))
            .expect(1)
            .mount(&server)
            .await;

        let client = ApiClient::new(&test_config(&server.uri())).unwrap();
        let err = run(&client, PipelineCmd::List).await.unwrap_err();

        let message = err.to_string();
        assert!(
            message.contains("does not serve pipelines"),
            "error was: {message}"
        );
        for banned in ["skardi-cloud", "gateway", "cloud"] {
            assert!(
                !message.to_lowercase().contains(banned),
                "error mentions a product/deploy term ({banned}): {message}"
            );
        }
    }

    // -- show: 404 + /pipelines also 404s -> no-pipeline-surface message --

    #[tokio::test]
    async fn show_404_with_no_pipeline_surface_yields_honest_message() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/pipeline/ghost"))
            .respond_with(ResponseTemplate::new(404).set_body_json(json!({
                "success": false,
                "error": "not found",
                "error_type": "not_found",
                "details": null,
                "timestamp": "2026-07-23T00:00:00Z",
            })))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/pipelines"))
            .respond_with(ResponseTemplate::new(404).set_body_json(json!({
                "success": false,
                "error": "not found",
                "error_type": "not_found",
                "details": null,
                "timestamp": "2026-07-23T00:00:00Z",
            })))
            .expect(1)
            .mount(&server)
            .await;

        let client = ApiClient::new(&test_config(&server.uri())).unwrap();
        let err = run(
            &client,
            PipelineCmd::Show {
                name: "ghost".to_string(),
            },
        )
        .await
        .unwrap_err();

        let message = err.to_string();
        assert!(
            message.contains("does not serve pipelines"),
            "error was: {message}"
        );
    }

    // -- show: 404 + /pipelines succeeds -> pipeline genuinely missing ----

    #[tokio::test]
    async fn show_404_with_pipeline_surface_present_names_pipeline() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/pipeline/ghost"))
            .respond_with(ResponseTemplate::new(404).set_body_json(json!({
                "success": false,
                "error": "not found",
                "error_type": "not_found",
                "details": null,
                "timestamp": "2026-07-23T00:00:00Z",
            })))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/pipelines"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!([])))
            .expect(1)
            .mount(&server)
            .await;

        let client = ApiClient::new(&test_config(&server.uri())).unwrap();
        let err = run(
            &client,
            PipelineCmd::Show {
                name: "ghost".to_string(),
            },
        )
        .await
        .unwrap_err();

        let message = err.to_string();
        assert!(
            message.contains("pipeline 'ghost' not found"),
            "error was: {message}"
        );
    }

    // -- show: 404 + probe fails for an unrelated reason (500): fall back -

    #[tokio::test]
    async fn show_404_with_probe_server_error_falls_back_to_not_found() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/pipeline/ghost"))
            .respond_with(ResponseTemplate::new(404).set_body_json(json!({
                "success": false,
                "error": "not found",
                "error_type": "not_found",
                "details": null,
                "timestamp": "2026-07-23T00:00:00Z",
            })))
            .expect(1)
            .mount(&server)
            .await;
        // A transient 500 on the probe says nothing about whether
        // pipelines are served — it must not be read as "no surface".
        Mock::given(method("GET"))
            .and(path("/pipelines"))
            .respond_with(ResponseTemplate::new(500).set_body_string("internal error"))
            .expect(1)
            .mount(&server)
            .await;

        let client = ApiClient::new(&test_config(&server.uri())).unwrap();
        let err = run(
            &client,
            PipelineCmd::Show {
                name: "ghost".to_string(),
            },
        )
        .await
        .unwrap_err();

        let message = err.to_string();
        assert!(
            message.contains("pipeline 'ghost' not found"),
            "error was: {message}"
        );
        assert!(
            !message.contains("does not serve pipelines"),
            "a probe 500 must not be read as proof the server lacks pipelines: {message}"
        );
    }

    // -- show: 404 + probe can't even connect: fall back too -------------

    #[tokio::test]
    async fn show_404_with_probe_connection_failure_falls_back_to_not_found() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/pipeline/ghost"))
            .respond_with(ResponseTemplate::new(404).set_body_json(json!({
                "success": false,
                "error": "not found",
                "error_type": "not_found",
                "details": null,
                "timestamp": "2026-07-23T00:00:00Z",
            })))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/pipelines"))
            .respond_with_err(|_req: &wiremock::Request| {
                std::io::Error::new(std::io::ErrorKind::ConnectionReset, "simulated reset")
            })
            .expect(1)
            .mount(&server)
            .await;

        let client = ApiClient::new(&test_config(&server.uri())).unwrap();
        let err = run(
            &client,
            PipelineCmd::Show {
                name: "ghost".to_string(),
            },
        )
        .await
        .unwrap_err();

        let message = err.to_string();
        assert!(
            message.contains("pipeline 'ghost' not found"),
            "error was: {message}"
        );
        assert!(
            !message.contains("does not serve pipelines"),
            "a probe connection failure must not be read as proof the server lacks pipelines: {message}"
        );
    }
}
