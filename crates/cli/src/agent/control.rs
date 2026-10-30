//! The HTTP client for the gateway's `/control/v1` door (design 2026-10-27
//! §2.3.2).
//!
//! The door re-roots `/control/v1/<rest>` to skardi-global's `/v1/<rest>` on
//! the control listener, so a route here is the control table's own path with
//! `/v1` swapped for `/control/v1`. Both hops refuse in one envelope,
//! `{"error": {"code", "message"}}` (§2.3.2), which this copies through
//! untouched; anything else — a proxy's HTML page, a gateway that does not
//! serve the door at all — becomes an envelope of the same shape here, so a
//! caller reads one.
//!
//! # Which failure is which exit
//!
//! | Answer | Exit |
//! | --- | --- |
//! | `401 token_unknown_or_revoked`, `403 control_required` | 6: the control token is gone, expired or never had `control` |
//! | any other 4xx | 3 |
//! | 5xx, no answer, or no door at all | 5 |
//!
//! # `429` and `Retry-After`
//!
//! The listener limits each token (§2.3.3) and answers `429 rate_limited`
//! with `Retry-After` in whole seconds. Such a refusal is answered before any
//! handler runs, so the request is retried after the wait — at most
//! [`RATE_LIMIT_RETRIES`] times, and never past [`MAX_RETRY_WAIT`] — before
//! the refusal is reported. Other `429`s (`handoff_limit`: five open
//! handoffs) are refusals a wait does not fix, and are reported at once.

use super::{AgentError, EXIT_NO_CONTROL, EXIT_REFUSED, EXIT_UNAVAILABLE, LOGIN_CONTROL};
use crate::identity::{ClientIdentity, Surface};
use reqwest::header::{AUTHORIZATION, HeaderValue, RETRY_AFTER};
use reqwest::{Client, Method, Response};
use serde_json::{Value, json};
use std::time::Duration;

/// The door's own upstream timeout is 120 seconds (an install submit runs the
/// permission check inside the request); this waits a little longer, so the
/// door's `503` arrives rather than a local timeout that hides it.
pub const REQUEST_TIMEOUT: Duration = Duration::from_secs(130);

/// How many times a `429 rate_limited` is retried after its `Retry-After`.
pub const RATE_LIMIT_RETRIES: u32 = 2;

/// The longest single wait a `Retry-After` may impose before the refusal is
/// reported instead. The listener's window is a minute.
pub const MAX_RETRY_WAIT: Duration = Duration::from_secs(60);

/// The scope-format header every install sends (§2.2.4): the server merges a
/// grant's scope only over content-type keys, and refuses a grant's install
/// without it (`400 scope_format_required`).
pub const SCOPE_FORMAT_HEADER: &str = "x-skardi-scope-format";
pub const SCOPE_FORMAT: &str = "content-types-v1";

/// Cap on a buffered control-plane answer. A picker page is the largest, and
/// is far below this.
const MAX_BODY_BYTES: usize = 8 * 1024 * 1024;

/// How much of an unrecognised error body becomes the message.
const MAX_SNIPPET_CHARS: usize = 200;

/// One workspace's control surface, under one control token.
pub struct Control {
    http: Client,
    /// `<gateway>/control/v1/workspaces/<ws>`, with no trailing slash.
    base: String,
    auth: HeaderValue,
    /// One `Retry-After` second, as slept. A second in production; a test
    /// shrinks it so the retry is exercised without a real wait.
    pub retry_unit: Duration,
}

impl Control {
    pub fn new(gateway: &str, workspace: &str, control_token: &str) -> Result<Control, AgentError> {
        let http = Client::builder()
            .no_proxy()
            .timeout(REQUEST_TIMEOUT)
            .default_headers(ClientIdentity::from_env(Surface::Cli).headers())
            .build()
            .map_err(|err| AgentError::local(EXIT_UNAVAILABLE, "client_error", format!("{err}")))?;
        let mut auth = HeaderValue::from_str(&format!("Bearer {control_token}")).map_err(|_| {
            AgentError::local(
                EXIT_NO_CONTROL,
                "control_token_unsendable",
                "the stored control token cannot be sent in an HTTP header; run `skardi login --control` again",
            )
            .with_next(LOGIN_CONTROL)
        })?;
        auth.set_sensitive(true);
        let base = format!(
            "{}/control/v1/workspaces/{}",
            gateway.trim_end_matches('/'),
            crate::client::encode_component(workspace)
        );
        Ok(Control {
            http,
            base,
            auth,
            retry_unit: Duration::from_secs(1),
        })
    }

    /// `GET <base><path>?<query>`; `path` is empty or starts with `/`.
    pub async fn get(&self, path: &str, query: &[(&str, &str)]) -> Result<Value, AgentError> {
        self.send(Method::GET, path, query, None, &[]).await
    }

    /// `POST <base><path>` with an optional JSON body and extra headers.
    pub async fn post(
        &self,
        path: &str,
        body: Option<&Value>,
        headers: &[(&str, &str)],
    ) -> Result<Value, AgentError> {
        self.send(Method::POST, path, &[], body, headers).await
    }

    async fn send(
        &self,
        method: Method,
        path: &str,
        query: &[(&str, &str)],
        body: Option<&Value>,
        headers: &[(&str, &str)],
    ) -> Result<Value, AgentError> {
        let url = format!("{}{path}", self.base);
        let mut attempt = 0;
        loop {
            let mut request = self.http.request(method.clone(), &url);
            if !query.is_empty() {
                request = request.query(query);
            }
            if let Some(body) = body {
                request = request.json(body);
            }
            for (name, value) in headers {
                request = request.header(*name, *value);
            }
            request = request.header(AUTHORIZATION, self.auth.clone());
            let response = request
                .send()
                .await
                .map_err(|err| unreachable(&url, &err))?;
            match read(response, &url).await {
                Err(err) if err.code() == "rate_limited" && attempt < RATE_LIMIT_RETRIES => {
                    let wait = retry_after_of(&err)
                        .and_then(|seconds| u32::try_from(seconds).ok())
                        .map(|seconds| self.retry_unit.saturating_mul(seconds))
                        .filter(|wait| *wait <= MAX_RETRY_WAIT);
                    let Some(wait) = wait else {
                        return Err(err);
                    };
                    attempt += 1;
                    tokio::time::sleep(wait).await;
                }
                other => return other,
            }
        }
    }
}

/// The `Retry-After` a refusal carried, as [`read`] recorded it.
fn retry_after_of(err: &AgentError) -> Option<u64> {
    err.error.get("retry_after").and_then(Value::as_u64)
}

fn unreachable(url: &str, err: &reqwest::Error) -> AgentError {
    AgentError::local(
        EXIT_UNAVAILABLE,
        "control_plane_unreachable",
        format!("cannot reach the gateway's control door at {url}: {err}"),
    )
}

/// Read one answer: the body on success, or the refusal as an [`AgentError`].
async fn read(mut response: Response, url: &str) -> Result<Value, AgentError> {
    let status = response.status().as_u16();
    let retry_after = response
        .headers()
        .get(RETRY_AFTER)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.trim().parse::<u64>().ok());
    let mut buf: Vec<u8> = Vec::new();
    loop {
        match response.chunk().await {
            Ok(Some(chunk)) => {
                if buf.len().saturating_add(chunk.len()) > MAX_BODY_BYTES {
                    return Err(AgentError::local(
                        EXIT_UNAVAILABLE,
                        "response_too_large",
                        format!("{url} returned more than {MAX_BODY_BYTES} bytes"),
                    ));
                }
                buf.extend_from_slice(&chunk);
            }
            Ok(None) => break,
            Err(err) => return Err(unreachable(url, &err)),
        }
    }
    let text = String::from_utf8_lossy(&buf);
    if (200..300).contains(&status) {
        if text.trim().is_empty() {
            return Ok(Value::Null);
        }
        return serde_json::from_str(&text).map_err(|err| {
            AgentError::local(
                EXIT_UNAVAILABLE,
                "invalid_response",
                format!("{url} answered {status} with a body that is not JSON: {err}"),
            )
        });
    }
    Err(refusal(status, &text, retry_after))
}

/// Map a non-success answer to an [`AgentError`], copying the server's
/// envelope through when it is one.
pub fn refusal(status: u16, text: &str, retry_after: Option<u64>) -> AgentError {
    let envelope = serde_json::from_str::<Value>(text)
        .ok()
        .and_then(|value| value.get("error").cloned())
        .filter(|error| error.get("code").and_then(Value::as_str).is_some());
    let mut err = match envelope {
        Some(error) => AgentError {
            exit: exit_for(status, error.get("code").and_then(Value::as_str)),
            error,
            next: None,
        },
        // A gateway that does not mount the door at all answers its own 404,
        // which is not the envelope: that is a deployment with the control
        // surface off, not a refusal of this request.
        None if status == 404 => AgentError::local(
            EXIT_UNAVAILABLE,
            "control_surface_unavailable",
            "this gateway does not serve /control/v1 — the deployment's control surface is off, or the context points at something other than a skardi-cloud gateway",
        ),
        None => AgentError {
            exit: exit_for(status, None),
            error: json!({
                "code": format!("http_{status}"),
                "message": snippet(text, status),
            }),
            next: None,
        },
    };
    if err.exit == EXIT_NO_CONTROL {
        err.next = Some(LOGIN_CONTROL.to_string());
    }
    if let Some(seconds) = retry_after {
        err = err.with_field("retry_after", json!(seconds));
    }
    err
}

/// §2.4.2's table, by status and code.
fn exit_for(status: u16, code: Option<&str>) -> u8 {
    match (status, code) {
        (401, Some("token_unknown_or_revoked")) | (403, Some("control_required")) => {
            EXIT_NO_CONTROL
        }
        (500..=599, _) => EXIT_UNAVAILABLE,
        _ => EXIT_REFUSED,
    }
}

fn snippet(text: &str, status: u16) -> String {
    let line = text.lines().next().unwrap_or_default().trim();
    if line.is_empty() {
        return format!("the control plane answered HTTP {status}");
    }
    if line.chars().count() <= MAX_SNIPPET_CHARS {
        return line.to_string();
    }
    let head: String = line.chars().take(MAX_SNIPPET_CHARS).collect();
    format!("{head}… (truncated)")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent::{EXIT_REFUSED, EXIT_UNAVAILABLE};
    use rstest::rstest;
    use wiremock::matchers::{header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    const CONTROL: &str = "skardi_pat_control_secret";

    fn control(server: &MockServer) -> Control {
        let mut control = Control::new(&server.uri(), "acme-prod", CONTROL).unwrap();
        control.retry_unit = Duration::from_millis(1);
        control
    }

    /// The door's path is the control table's with `/v1` swapped for
    /// `/control/v1`, and the bearer is the control token.
    #[tokio::test]
    async fn requests_go_to_the_door_with_the_control_bearer() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/control/v1/workspaces/acme-prod"))
            .and(header(
                "authorization",
                format!("Bearer {CONTROL}").as_str(),
            ))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(json!({"tenant_slug": "acme-prod"})),
            )
            .expect(1)
            .mount(&server)
            .await;
        let body = control(&server).get("", &[]).await.unwrap();
        assert_eq!(body["tenant_slug"], "acme-prod");
    }

    /// Both hops refuse in one envelope, and it is copied through with every
    /// field it carried.
    #[rstest]
    #[case::listener_refusal(
        403,
        r#"{"error":{"code":"admin_required","message":"admins only","workspace":"acme-prod"}}"#,
        "admin_required",
        EXIT_REFUSED
    )]
    #[case::door_refusal(
        401,
        r#"{"error":{"code":"control_requires_pat","message":"a PAT"}}"#,
        "control_requires_pat",
        EXIT_REFUSED
    )]
    #[case::expired_control(
        401,
        r#"{"error":{"code":"token_unknown_or_revoked","message":"run skardi login --control"}}"#,
        "token_unknown_or_revoked",
        EXIT_NO_CONTROL
    )]
    #[case::no_control(
        403,
        r#"{"error":{"code":"control_required","message":"not a control token"}}"#,
        "control_required",
        EXIT_NO_CONTROL
    )]
    #[case::listener_down(
        503,
        r#"{"error":{"code":"control_plane_unavailable","message":"down"}}"#,
        "control_plane_unavailable",
        EXIT_UNAVAILABLE
    )]
    #[case::html_502(502, "<html>bad gateway</html>", "http_502", EXIT_UNAVAILABLE)]
    #[case::no_door(404, "not found", "control_surface_unavailable", EXIT_UNAVAILABLE)]
    #[case::off_table(
        404,
        r#"{"error":{"code":"not_found","message":"no such route"}}"#,
        "not_found",
        EXIT_REFUSED
    )]
    fn refusals_map_to_the_designs_exit_codes(
        #[case] status: u16,
        #[case] body: &str,
        #[case] code: &str,
        #[case] exit: u8,
    ) {
        let err = refusal(status, body, None);
        assert_eq!(err.code(), code, "{err:?}");
        assert_eq!(err.exit, exit, "{err:?}");
        if exit == EXIT_NO_CONTROL {
            assert_eq!(err.next.as_deref(), Some(LOGIN_CONTROL));
        }
    }

    #[test]
    fn the_servers_extra_fields_survive_the_copy() {
        let err = refusal(
            409,
            r#"{"error":{"code":"target_exists","message":"connected","alias":"acme-notion"}}"#,
            None,
        );
        assert_eq!(err.error["alias"], "acme-notion");
    }

    /// `429 rate_limited` is retried after its `Retry-After`, and succeeds.
    #[tokio::test]
    async fn a_rate_limit_is_retried_after_its_retry_after() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/control/v1/workspaces/acme-prod/connections"))
            .respond_with(
                ResponseTemplate::new(429)
                    .insert_header("retry-after", "2")
                    .set_body_json(
                        json!({"error": {"code": "rate_limited", "message": "slow down"}}),
                    ),
            )
            .up_to_n_times(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/control/v1/workspaces/acme-prod/connections"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!([])))
            .mount(&server)
            .await;
        let body = control(&server).get("/connections", &[]).await.unwrap();
        assert_eq!(body, json!([]));
        assert_eq!(server.received_requests().await.unwrap().len(), 2);
    }

    /// Past the retries the refusal is reported, with the wait it named.
    #[tokio::test]
    async fn a_persistent_rate_limit_is_reported_with_its_retry_after() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(
                ResponseTemplate::new(429)
                    .insert_header("retry-after", "1")
                    .set_body_json(
                        json!({"error": {"code": "rate_limited", "message": "slow down"}}),
                    ),
            )
            .mount(&server)
            .await;
        let err = control(&server).get("/connections", &[]).await.unwrap_err();
        assert_eq!(err.code(), "rate_limited");
        assert_eq!(err.exit, EXIT_REFUSED);
        assert_eq!(err.error["retry_after"], 1);
        assert_eq!(
            server.received_requests().await.unwrap().len(),
            1 + RATE_LIMIT_RETRIES as usize
        );
    }

    /// Another `429` — five handoffs already open — is not a wait.
    #[tokio::test]
    async fn a_handoff_limit_is_not_retried() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(
                ResponseTemplate::new(429)
                    .insert_header("retry-after", "1")
                    .set_body_json(
                        json!({"error": {"code": "handoff_limit", "message": "five open"}}),
                    ),
            )
            .mount(&server)
            .await;
        let err = control(&server)
            .post("/handoffs", Some(&json!({})), &[])
            .await
            .unwrap_err();
        assert_eq!(err.code(), "handoff_limit");
        assert_eq!(server.received_requests().await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn an_unreachable_gateway_is_exit_5() {
        let control = Control::new("http://127.0.0.1:1", "acme-prod", CONTROL).unwrap();
        let err = control.get("", &[]).await.unwrap_err();
        assert_eq!(err.exit, EXIT_UNAVAILABLE);
        assert_eq!(err.code(), "control_plane_unreachable");
        assert!(!err.message().contains(CONTROL), "{err:?}");
    }
}
