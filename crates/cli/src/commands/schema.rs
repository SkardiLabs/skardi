//! `skardi schema` — fetch the server's data source schema via
//! `GET /data_source` and pretty-print the response.

use crate::client::ApiClient;
use anyhow::Result;

/// Run `skardi schema`: `GET /data_source` and pretty-print the response.
pub async fn run(client: &ApiClient) -> Result<()> {
    let response = client.get("/data_source").await?;
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
    async fn run_hits_data_source_endpoint() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/data_source"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"tables": []})))
            .expect(1)
            .mount(&server)
            .await;

        let client = ApiClient::new(&test_config(&server.uri())).unwrap();
        let result = run(&client).await;

        assert!(result.is_ok(), "expected Ok, got {result:?}");
    }
}
