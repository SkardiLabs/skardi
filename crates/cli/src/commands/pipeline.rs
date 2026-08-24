//! `skardi pipeline list|show` — list pipelines or show one pipeline's
//! definition via `GET /pipelines` / `GET /pipeline/<name>`, and pretty-print
//! the server's JSON response.

use crate::client::{ApiClient, encode_component};
use anyhow::Result;
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

/// Run `skardi pipeline <cmd>`: `GET /pipelines` for `List`, or
/// `GET /pipeline/<name>` for `Show`, and pretty-print the response.
pub async fn run(client: &ApiClient, cmd: PipelineCmd) -> Result<()> {
    let path = match &cmd {
        PipelineCmd::List => "/pipelines".to_string(),
        PipelineCmd::Show { name } => format!("/pipeline/{}", encode_component(name)),
    };

    let response = client.get(&path).await?;
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
}
