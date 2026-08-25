//! `skardi query` — send ad-hoc SQL to `POST /query` and render the result.

use crate::client::ApiClient;
use crate::output::print_result;
use anyhow::{Context, Result, bail};
use serde_json::{Value, json};
use std::path::PathBuf;

/// Run `skardi query`: resolve the SQL text to send (file wins over `-e`
/// when both are given), `POST` it to `/query`, and hand the response
/// envelope to [`print_result`].
pub async fn run(
    client: &ApiClient,
    sql: Option<String>,
    file: Option<PathBuf>,
    max_rows: Option<usize>,
    table: bool,
) -> Result<()> {
    let text = resolve_sql(sql, file)?;
    let body = build_body(&text, max_rows);

    let response = client.post("/query", &body).await?;
    print_result(&response, table);

    Ok(())
}

/// Resolve the SQL text to send: `file` (read from disk, with a contextual
/// error naming the path on failure) wins when both `sql` and `file` are
/// given; neither given is a client-side error, raised before any request
/// is made.
fn resolve_sql(sql: Option<String>, file: Option<PathBuf>) -> Result<String> {
    if let Some(path) = file {
        std::fs::read_to_string(&path)
            .with_context(|| format!("failed to read SQL file {}", path.display()))
    } else if let Some(sql) = sql {
        Ok(sql)
    } else {
        bail!("no SQL given: pass -e <SQL> or -f <FILE>")
    }
}

/// Build the `/query` request body: `{"sql": text}`, plus a `"max_rows"`
/// key only when `max_rows` was actually passed.
fn build_body(sql: &str, max_rows: Option<usize>) -> Value {
    let mut body = json!({ "sql": sql });
    if let Some(max_rows) = max_rows {
        body["max_rows"] = json!(max_rows);
    }
    body
}

#[cfg(test)]
mod tests {
    use super::{build_body, run};
    use crate::client::ApiClient;
    use crate::config::ClientConfig;
    use serde_json::json;
    use std::io::Write;
    use tempfile::NamedTempFile;
    use wiremock::matchers::{body_json, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn test_config(server: &str) -> ClientConfig {
        ClientConfig {
            server: server.to_string(),
            token: None,
            context: None,
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

    // -- 1. body-builder ------------------------------------------------

    #[test]
    fn build_body_without_max_rows_has_only_sql() {
        let body = build_body("select 1", None);
        assert_eq!(body, json!({"sql": "select 1"}));
    }

    #[test]
    fn build_body_with_max_rows_has_both_keys() {
        let body = build_body("select 1", Some(50));
        assert_eq!(body, json!({"sql": "select 1", "max_rows": 50}));
    }

    // -- 2. run posts the expected body and succeeds ---------------------

    #[tokio::test]
    async fn run_posts_expected_body_and_succeeds() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/query"))
            .and(body_json(json!({"sql": "select 1"})))
            .respond_with(ResponseTemplate::new(200).set_body_json(success_envelope()))
            .expect(1)
            .mount(&server)
            .await;

        let client = ApiClient::new(&test_config(&server.uri())).unwrap();
        let result = run(&client, Some("select 1".to_string()), None, None, false).await;

        assert!(result.is_ok(), "expected Ok, got {result:?}");
    }

    // -- 3. file takes precedence over -e --------------------------------

    #[tokio::test]
    async fn file_takes_precedence_over_inline_sql() {
        let server = MockServer::start().await;
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(b"select 2").unwrap();

        Mock::given(method("POST"))
            .and(path("/query"))
            .and(body_json(json!({"sql": "select 2"})))
            .respond_with(ResponseTemplate::new(200).set_body_json(success_envelope()))
            .expect(1)
            .mount(&server)
            .await;

        let client = ApiClient::new(&test_config(&server.uri())).unwrap();
        let result = run(
            &client,
            Some("select 1".to_string()),
            Some(file.path().to_path_buf()),
            None,
            false,
        )
        .await;

        assert!(result.is_ok(), "expected Ok, got {result:?}");
    }

    // -- 4. neither -e nor -f is a client-side error, no request made -----

    #[tokio::test]
    async fn missing_sql_errors_without_request() {
        // Unreachable server: proves no request was attempted, since a
        // real request here would surface as a connect error, not this one.
        let client = ApiClient::new(&test_config("http://127.0.0.1:1")).unwrap();

        let err = run(&client, None, None, None, false).await.unwrap_err();

        assert!(err.to_string().contains("no SQL given"), "error was: {err}");
    }

    // -- 5. server error envelope propagates with error_type -------------

    #[tokio::test]
    async fn server_400_envelope_error_contains_error_type() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/query"))
            .respond_with(ResponseTemplate::new(400).set_body_json(json!({
                "success": false,
                "error": "column foo does not exist",
                "error_type": "sql_validation_error",
                "details": null,
                "timestamp": "2026-07-23T00:00:00Z",
            })))
            .mount(&server)
            .await;

        let client = ApiClient::new(&test_config(&server.uri())).unwrap();
        let err = run(&client, Some("select foo".to_string()), None, None, false)
            .await
            .unwrap_err();

        assert!(
            err.to_string().contains("sql_validation_error"),
            "error was: {err}"
        );
    }
}
