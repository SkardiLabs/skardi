//! `skardi query` — send ad-hoc SQL to `POST /query` and render the result.

use crate::client::ApiClient;
use crate::output::print_result;
use crate::session::MAX_SESSION_ID_CHARS;
use anyhow::{Context, Result, bail};
use serde_json::{Value, json};
use std::path::PathBuf;

/// Maximum `ai_context.purpose` length, in characters. Restates the server's
/// `query_handlers::MAX_PURPOSE_CHARS` under the same name — `skardi-cli`
/// does not depend on the server crate, so this cannot be a shared item, but
/// an identical name keeps `grep MAX_PURPOSE_CHARS` finding every site that
/// must move together.
const MAX_PURPOSE_CHARS: usize = 2000;

/// Run `skardi query`: resolve the SQL text to send (file wins over `-e`
/// when both are given), `POST` it to `/query`, and hand the response
/// envelope to [`print_result`].
pub async fn run(
    client: &ApiClient,
    sql: Option<String>,
    file: Option<PathBuf>,
    max_rows: Option<usize>,
    table: bool,
    purpose: Option<String>,
    session_id: Option<String>,
) -> Result<()> {
    let text = resolve_sql(sql, file)?;
    let ai_context = build_ai_context(purpose, session_id)?;
    let body = build_body(&text, max_rows, ai_context);

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

/// Build the `/query` request body: `{"sql": text}`, plus `"max_rows"` and
/// `"ai_context"` keys only when those were actually passed.
fn build_body(sql: &str, max_rows: Option<usize>, ai_context: Option<Value>) -> Value {
    let mut body = json!({ "sql": sql });
    if let Some(max_rows) = max_rows {
        body["max_rows"] = json!(max_rows);
    }
    if let Some(ai_context) = ai_context {
        body["ai_context"] = ai_context;
    }
    body
}

/// Build the optional `ai_context` object from the `--purpose` /
/// `--session-id` pair: `Ok(None)` when neither was given, `Err` when a value
/// is present that the server would reject.
///
/// The pair is all-or-nothing because the server's `validate_ai_context`
/// requires both fields once `ai_context` is present at all. Clap's
/// `requires` already rejects one-without-the-other at parse time; the arms
/// below keep this function honest for any other caller rather than silently
/// dropping a half-filled context.
///
/// Deliberately NOT reusing [`crate::session::validate_session_id`]. That
/// predicate mirrors the `X-Skardi-Session-Id` *header* rules — visible ASCII
/// only, no space, no comma — which exist because an intermediary may reshape
/// a header value. `/query` carries the session id inside the JSON body,
/// where the server asks only for a non-empty string within the cap, so
/// applying the header predicate here would reject values the server accepts.
/// Only the cap is shared, hence the constant import.
fn build_ai_context(purpose: Option<String>, session_id: Option<String>) -> Result<Option<Value>> {
    match (purpose, session_id) {
        (None, None) => Ok(None),
        (Some(purpose), Some(session_id)) => {
            validate_context_string(&purpose, "--purpose", MAX_PURPOSE_CHARS)?;
            validate_context_string(&session_id, "--session-id", MAX_SESSION_ID_CHARS)?;
            Ok(Some(
                json!({ "purpose": purpose, "session_id": session_id }),
            ))
        }
        (Some(_), None) => bail!("--purpose requires --session-id"),
        (None, Some(_)) => bail!("--session-id requires --purpose"),
    }
}

/// Require a context value to be non-empty and within `max_chars`, naming the
/// flag it came from so the message points at what to fix.
fn validate_context_string(value: &str, flag: &str, max_chars: usize) -> Result<()> {
    if value.is_empty() {
        bail!("{flag} must not be empty");
    }
    if value.chars().count() > max_chars {
        bail!("{flag} must be at most {max_chars} characters");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{MAX_PURPOSE_CHARS, MAX_SESSION_ID_CHARS, build_ai_context, build_body, run};
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
        let body = build_body("select 1", None, None);
        assert_eq!(body, json!({"sql": "select 1"}));
    }

    #[test]
    fn build_body_with_max_rows_has_both_keys() {
        let body = build_body("select 1", Some(50), None);
        assert_eq!(body, json!({"sql": "select 1", "max_rows": 50}));
    }

    #[test]
    fn build_body_with_ai_context_carries_it_verbatim() {
        let ctx = json!({"purpose": "weekly PR ageing", "session_id": "sess-9"});
        let body = build_body("select 1", None, Some(ctx.clone()));
        assert_eq!(body, json!({"sql": "select 1", "ai_context": ctx}));
    }

    // -- 1b. ai_context builder ------------------------------------------

    #[test]
    fn neither_flag_yields_no_ai_context() {
        assert_eq!(build_ai_context(None, None).unwrap(), None);
    }

    #[test]
    fn both_flags_yield_the_object_the_server_validates() {
        let ctx = build_ai_context(Some("count paid orders".into()), Some("sess-1".into()))
            .unwrap()
            .expect("expected an ai_context");
        assert_eq!(
            ctx,
            json!({"purpose": "count paid orders", "session_id": "sess-1"})
        );
    }

    #[test]
    fn one_flag_without_the_other_is_an_error() {
        // Clap's `requires` catches this at parse time; the builder stays
        // honest for any other caller instead of dropping half a context.
        let err = build_ai_context(Some("p".into()), None).unwrap_err();
        assert!(
            err.to_string().contains("--purpose requires --session-id"),
            "error was: {err}"
        );

        let err = build_ai_context(None, Some("s".into())).unwrap_err();
        assert!(
            err.to_string().contains("--session-id requires --purpose"),
            "error was: {err}"
        );
    }

    #[test]
    fn empty_or_oversized_values_are_rejected_naming_the_flag() {
        let cases = [
            (String::new(), "sess-1".to_string(), "--purpose"),
            ("p".to_string(), String::new(), "--session-id"),
            (
                "x".repeat(MAX_PURPOSE_CHARS + 1),
                "sess-1".to_string(),
                "--purpose",
            ),
            (
                "p".to_string(),
                "x".repeat(MAX_SESSION_ID_CHARS + 1),
                "--session-id",
            ),
        ];

        for (purpose, session_id, flag) in cases {
            let err = build_ai_context(Some(purpose), Some(session_id)).unwrap_err();
            assert!(
                err.to_string().contains(flag),
                "expected a {flag} message, got: {err}"
            );
        }
    }

    #[test]
    fn body_session_ids_are_not_held_to_the_header_predicate() {
        // `/query` carries the session id in JSON, not in a header, so the
        // spaces / non-ASCII / comma rules of `session::validate_session_id`
        // must NOT apply here — the server accepts any non-empty string
        // within the cap, and rejecting more than it does would be a
        // client-side regression invisible from the server side.
        for sid in ["sess 1", "会话-1", "sess-1, sess-2", "  padded  "] {
            let ctx = build_ai_context(Some("p".into()), Some(sid.to_string()))
                .unwrap()
                .expect("expected an ai_context");
            assert_eq!(ctx["session_id"], json!(sid));
        }
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
        let result = run(
            &client,
            Some("select 1".to_string()),
            None,
            None,
            false,
            None,
            None,
        )
        .await;

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
            None,
            None,
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

        let err = run(&client, None, None, None, false, None, None)
            .await
            .unwrap_err();

        assert!(err.to_string().contains("no SQL given"), "error was: {err}");
    }

    // -- 4b. --purpose/--session-id reach the wire as ai_context ---------

    #[tokio::test]
    async fn run_with_purpose_and_session_id_posts_ai_context() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/query"))
            .and(body_json(json!({
                "sql": "select 1",
                "ai_context": {"purpose": "count paid orders", "session_id": "sess-1"},
            })))
            .respond_with(ResponseTemplate::new(200).set_body_json(success_envelope()))
            .expect(1)
            .mount(&server)
            .await;

        let client = ApiClient::new(&test_config(&server.uri())).unwrap();
        let result = run(
            &client,
            Some("select 1".to_string()),
            None,
            None,
            false,
            Some("count paid orders".to_string()),
            Some("sess-1".to_string()),
        )
        .await;

        assert!(result.is_ok(), "expected Ok, got {result:?}");
    }

    // -- 4c. a rejected context never reaches the server ------------------

    #[tokio::test]
    async fn invalid_context_errors_without_contacting_server() {
        let server = MockServer::start().await;
        // expect(0): the check must fail before the request, otherwise the
        // server's 400 round trip is what the user waits for.
        Mock::given(method("POST"))
            .and(path("/query"))
            .respond_with(ResponseTemplate::new(200).set_body_json(success_envelope()))
            .expect(0)
            .mount(&server)
            .await;

        let client = ApiClient::new(&test_config(&server.uri())).unwrap();
        let err = run(
            &client,
            Some("select 1".to_string()),
            None,
            None,
            false,
            Some(String::new()),
            Some("sess-1".to_string()),
        )
        .await
        .unwrap_err();

        assert!(err.to_string().contains("--purpose"), "error was: {err}");
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
        let err = run(
            &client,
            Some("select foo".to_string()),
            None,
            None,
            false,
            None,
            None,
        )
        .await
        .unwrap_err();

        assert!(
            err.to_string().contains("sql_validation_error"),
            "error was: {err}"
        );
    }
}
