//! `skardi run <name>` — execute a named server pipeline via
//! `POST /{name}/execute` and render the result.

use crate::client::{ApiClient, ApiError};
use crate::output::print_result;
use crate::params::build_body;
use anyhow::{Result, anyhow};
use serde_json::Value;

/// Run `skardi run <name>`: build the request body from `-d`/`-p`, `POST` it
/// to `/{name}/execute`, and hand the response envelope to [`print_result`].
///
/// A 404 response is remapped to a friendly "pipeline not found" error
/// naming `name`; every other `ApiError` passes through unchanged (so, e.g.,
/// `main`'s `downcast_ref::<ApiError>` exit-code mapping for connect
/// failures keeps working).
pub async fn run(
    client: &ApiClient,
    name: &str,
    data: Option<&str>,
    param_flags: &[String],
    table: bool,
) -> Result<()> {
    let body = build_body(data, param_flags)?;
    let path = format!("/{name}/execute");

    match client.post(&path, &Value::Object(body)).await {
        Ok(response) => {
            print_result(&response, table);
            Ok(())
        }
        Err(ApiError::Http { status: 404, .. }) => Err(anyhow!(
            "pipeline '{name}' not found — try 'skardi pipeline list'"
        )),
        Err(err) => Err(err.into()),
    }
}

#[cfg(test)]
mod tests {
    use super::run;
    use crate::client::ApiClient;
    use crate::config::ClientConfig;
    use serde_json::json;
    use wiremock::matchers::{body_json, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn test_config(server: &str) -> ClientConfig {
        ClientConfig {
            server: server.to_string(),
            token: None,
        }
    }

    fn success_envelope() -> serde_json::Value {
        json!({
            "success": true,
            "data": [{"n": 1}],
            "rows": 1,
            "truncated": false,
        })
    }

    // -- 1. -p overrides -d, exact merged body posted --------------------

    #[tokio::test]
    async fn param_overrides_data_key_in_posted_body() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/daily_report/execute"))
            .and(body_json(json!({"user_id": 1, "category": "premium"})))
            .respond_with(ResponseTemplate::new(200).set_body_json(success_envelope()))
            .expect(1)
            .mount(&server)
            .await;

        let client = ApiClient::new(&test_config(&server.uri())).unwrap();
        let result = run(
            &client,
            "daily_report",
            Some(r#"{"user_id":1,"category":"basic"}"#),
            &["category=premium".to_string()],
            false,
        )
        .await;

        assert!(result.is_ok(), "expected Ok, got {result:?}");
    }

    // -- 2. no -d/-p posts exactly {} ------------------------------------

    #[tokio::test]
    async fn no_data_or_params_posts_empty_object() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/daily_report/execute"))
            .and(body_json(json!({})))
            .respond_with(ResponseTemplate::new(200).set_body_json(success_envelope()))
            .expect(1)
            .mount(&server)
            .await;

        let client = ApiClient::new(&test_config(&server.uri())).unwrap();
        let result = run(&client, "daily_report", None, &[], false).await;

        assert!(result.is_ok(), "expected Ok, got {result:?}");
    }

    // -- 3. 404 remapped to friendly "pipeline not found" error ----------

    #[tokio::test]
    async fn not_found_status_yields_friendly_error() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/ghost/execute"))
            .respond_with(ResponseTemplate::new(404).set_body_json(json!({
                "success": false,
                "error": "pipeline not found",
                "error_type": "not_found",
                "details": null,
                "timestamp": "2026-07-23T00:00:00Z",
            })))
            .mount(&server)
            .await;

        let client = ApiClient::new(&test_config(&server.uri())).unwrap();
        let err = run(&client, "ghost", None, &[], false).await.unwrap_err();

        let message = err.to_string();
        assert!(
            message.contains("pipeline 'ghost' not found"),
            "error was: {message}"
        );
        assert!(
            message.contains("skardi pipeline list"),
            "error was: {message}"
        );
    }
}
