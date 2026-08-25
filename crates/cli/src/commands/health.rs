//! `skardi health [name]` — fetch overall server health via `GET /health`,
//! or one pipeline's health via `GET /health/<name>`, and pretty-print the
//! response.

use crate::client::{ApiClient, encode_component};
use anyhow::Result;

/// Run `skardi health [name]`: `GET /health` when `name` is `None`, or
/// `GET /health/<name>` otherwise, and pretty-print the response.
pub async fn run(client: &ApiClient, name: Option<&str>) -> Result<()> {
    let path = match name {
        Some(name) => format!("/health/{}", encode_component(name)),
        None => "/health".to_string(),
    };

    let response = client.get(&path).await?;
    let pretty = serde_json::to_string_pretty(&response)?;
    println!("{pretty}");

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::run;
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
    async fn no_name_hits_health_endpoint() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/health"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"status": "ok"})))
            .expect(1)
            .mount(&server)
            .await;

        let client = ApiClient::new(&test_config(&server.uri())).unwrap();
        let result = run(&client, None).await;

        assert!(result.is_ok(), "expected Ok, got {result:?}");
    }

    #[tokio::test]
    async fn name_hits_health_name_endpoint() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/health/daily_report"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"status": "ok"})))
            .expect(1)
            .mount(&server)
            .await;

        let client = ApiClient::new(&test_config(&server.uri())).unwrap();
        let result = run(&client, Some("daily_report")).await;

        assert!(result.is_ok(), "expected Ok, got {result:?}");
    }
}
