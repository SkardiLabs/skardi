//! `skardi run <name>` — execute a named server pipeline via
//! `POST /{name}/execute` and render the result.

use crate::client::{ApiClient, ApiError, encode_component};
use crate::output::print_result;
use crate::params::build_body;
use crate::session::validate_session_id;
use anyhow::{Result, anyhow};
use serde_json::Value;

/// Run `skardi run <name>`: build the request body from `-d`/`-p`, `POST` it
/// to `/{name}/execute`, and hand the response envelope to [`print_result`].
///
/// When `session_id` is set, it's sent as the `x-skardi-session-id` header
/// so the server records this execution against that session in its audit
/// ledger; when `None`, no such header is sent.
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
    session_id: Option<String>,
) -> Result<()> {
    if let Some(sid) = &session_id {
        validate_session_id(sid)?;
    }

    let body = build_body(data, param_flags)?;
    let path = format!("/{}/execute", encode_component(name));

    let response = match &session_id {
        Some(sid) => {
            client
                .post_with_headers(&path, &Value::Object(body), &[("x-skardi-session-id", sid)])
                .await
        }
        None => client.post(&path, &Value::Object(body)).await,
    };

    match response {
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
    use crate::client::{ApiClient, ApiError};
    use crate::config::ClientConfig;
    use serde_json::json;
    use wiremock::matchers::{body_json, header, method, path};
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
            None,
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
        let result = run(&client, "daily_report", None, &[], false, None).await;

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
        let err = run(&client, "ghost", None, &[], false, None)
            .await
            .unwrap_err();

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

    // -- 4. reserved characters in the name cannot change the route ------

    #[tokio::test]
    async fn name_with_reserved_characters_is_percent_encoded_in_path() {
        let server = MockServer::start().await;
        // wiremock matches on the raw (still-encoded) request path: a name
        // like "a/b c" must arrive as one encoded segment, not as the
        // two-segment path `/a/b c/execute`.
        Mock::given(method("POST"))
            .and(path("/a%2Fb%20c/execute"))
            .and(body_json(json!({})))
            .respond_with(ResponseTemplate::new(200).set_body_json(success_envelope()))
            .expect(1)
            .mount(&server)
            .await;

        let client = ApiClient::new(&test_config(&server.uri())).unwrap();
        run(&client, "a/b c", None, &[], false, None).await.unwrap();
    }

    // -- 5. --session-id sends X-Skardi-Session-Id header ----------------

    #[tokio::test]
    async fn run_with_session_id_sets_header() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/my-pipe/execute"))
            .and(header("x-skardi-session-id", "sess-9"))
            .respond_with(ResponseTemplate::new(200).set_body_json(success_envelope()))
            .expect(1)
            .mount(&server)
            .await;

        let client = ApiClient::new(&test_config(&server.uri())).unwrap();
        let result = run(
            &client,
            "my-pipe",
            None,
            &[],
            false,
            Some("sess-9".to_string()),
        )
        .await;

        assert!(result.is_ok(), "expected Ok, got {result:?}");
    }

    // -- 5b. invalid --session-id fails fast, before any request ----------

    #[tokio::test]
    async fn run_with_invalid_session_id_errors_without_contacting_server() {
        let server = MockServer::start().await;
        // expect(0): the whole point is that no request is ever sent — a
        // deferred header error would surface as ApiError::Connect and be
        // misread as "server unreachable".
        Mock::given(method("POST"))
            .and(path("/my-pipe/execute"))
            .respond_with(ResponseTemplate::new(200).set_body_json(success_envelope()))
            .expect(0)
            .mount(&server)
            .await;

        let client = ApiClient::new(&test_config(&server.uri())).unwrap();
        for bad in [
            "",
            &"x".repeat(201),
            "sesión-1",
            "line\nbreak",
            "tab\there",
            "sess-1, sess-2", // proxy-merged duplicate shape
            "   ",            // spaces-only
            "sess 1",         // interior space
        ] {
            let result = run(&client, "my-pipe", None, &[], false, Some(bad.to_string())).await;
            let err = result.expect_err("expected validation error");
            assert!(
                err.to_string().contains("--session-id"),
                "expected a --session-id validation message, got: {err}"
            );
            assert!(
                err.downcast_ref::<ApiError>().is_none(),
                "must not be an ApiError (would map to a connection exit code)"
            );
        }
    }

    // -- 6. no --session-id sends no header -------------------------------

    #[tokio::test]
    async fn run_without_session_id_sends_no_header() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/my-pipe/execute"))
            .respond_with(ResponseTemplate::new(200).set_body_json(success_envelope()))
            .expect(1)
            .mount(&server)
            .await;

        let client = ApiClient::new(&test_config(&server.uri())).unwrap();
        let result = run(&client, "my-pipe", None, &[], false, None).await;

        assert!(result.is_ok(), "expected Ok, got {result:?}");

        let requests = server.received_requests().await.unwrap();
        assert_eq!(requests.len(), 1);
        assert!(!requests[0].headers.contains_key("x-skardi-session-id"));
    }
}
