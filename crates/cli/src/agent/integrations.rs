//! `skardi integrations …` (design 2026-10-27 §2.4.1, rows 1, 2 and 4–9):
//! everything after the person's browser pass, as the console wizard does it,
//! over the control table.
//!
//! Each function makes the calls its row names and nothing else, and returns
//! an [`Outcome`] whose `next` is the following step of §2.7's sequence:
//!
//! ```text
//! list → connect → (handoff wait) → permissions → candidates… → install
//!      → status → sync
//! ```
//!
//! A connection version is always an argument, never remembered: the agent
//! names the version `handoff wait` handed it, and the server compares it
//! (§2.2.5).

use super::control::{Control, SCOPE_FORMAT, SCOPE_FORMAT_HEADER};
use super::handoff::{Polling, check_handoff_id, within};
use super::{AgentError, EXIT_REFUSED, EXIT_UNAVAILABLE, EXIT_USAGE, Outcome, scope, shell_quote};
use crate::client::encode_component;
use serde_json::{Value, json};
use std::time::Duration;
use tokio::time::{Instant, sleep};

/// The hint after `list`: one provider at a time, by the name `list` shows.
pub const CONNECT_HINT: &str = "skardi integrations connect <saas> --target new";

/// Row 1: the workspace's state, what this org may install, and what is
/// connected and syncing.
pub async fn list(control: &Control) -> Result<Outcome, AgentError> {
    let workspace = control.get("", &[]).await?;
    let availability = control.get("/integration-availability", &[]).await?;
    let connections = control.get("/connections", &[]).await?;
    let sources = control.get("/saas-sources", &[]).await?;
    let state = workspace
        .get("provisioning_state")
        .and_then(Value::as_str)
        .unwrap_or("unknown")
        .to_string();
    let count = |value: &Value| value.as_array().map_or(0, Vec::len);
    let sentence = format!(
        "workspace {} is {state}; {} connection(s), {} source(s)",
        workspace
            .get("tenant_slug")
            .and_then(Value::as_str)
            .unwrap_or("?"),
        count(&connections),
        count(&sources),
    );
    let result = json!({
        "workspace": workspace,
        "availability": availability,
        "connections": connections,
        "sources": sources,
    });
    let outcome = Outcome::new(result, sentence).showing_result();
    Ok(if state == "active" {
        outcome.with_next(CONNECT_HINT)
    } else {
        outcome.with_note(format!(
            "the workspace is {state}, not active: connect nothing until it is, and run this again"
        ))
    })
}

/// `--target`: `new`, or the alias of the connection to reconnect.
pub fn target_body(raw: &str) -> Result<Value, AgentError> {
    let target = raw.trim();
    if target.is_empty() {
        return Err(AgentError::usage(
            "target_required",
            "--target is `new`, or the alias of the connection to reconnect",
        ));
    }
    Ok(if target == "new" {
        json!({ "kind": "new" })
    } else {
        json!({ "kind": "existing", "alias": target })
    })
}

/// Row 2: open a handoff for one provider and print its page link.
pub async fn connect(control: &Control, saas: &str, target: &str) -> Result<Outcome, AgentError> {
    let saas = saas.trim();
    if saas.is_empty() {
        return Err(AgentError::usage(
            "invalid_saas",
            "name the provider, as `skardi integrations list` shows it",
        ));
    }
    let body = json!({ "saas": saas, "target": target_body(target)? });
    let created = control.post("/handoffs", Some(&body), &[]).await?;
    let (Some(id), Some(page_url)) = (
        created.get("handoff_id").and_then(Value::as_str),
        created.get("page_url").and_then(Value::as_str),
    ) else {
        return Err(AgentError::local(
            EXIT_UNAVAILABLE,
            "invalid_response",
            "the control plane opened a handoff but returned no id or page link",
        ));
    };
    let what = match created.get("kind").and_then(Value::as_str) {
        Some("secret_drop_credential") => "paste the token on that page — never in this chat",
        Some("secret_drop_oauth_client") => {
            "create an app at the provider, paste its client id and secret on that page, then consent"
        }
        _ => "consent at the provider from that page",
    };
    let expires = created
        .get("expires_at")
        .and_then(Value::as_str)
        .map(|at| format!(" The link works until {at} if unopened."))
        .unwrap_or_default();
    let sentence = format!(
        "Ask the person to open this link and finish there ({what}):\n  {page_url}\nIt is theirs alone: it opens only under their own sign-in.{expires}"
    );
    let next = format!("skardi handoff wait {}", shell_quote(id));
    Ok(Outcome::new(created.clone(), sentence).with_next(next))
}

/// A connection version is a UUID; anything else is caught here rather than
/// as a `400 invalid_body` that names a deserializer.
fn check_version(raw: &str) -> Result<String, AgentError> {
    uuid::Uuid::parse_str(raw.trim())
        .map(|id| id.to_string())
        .map_err(|_| {
            AgentError::local(
                EXIT_USAGE,
                "invalid_connection_version",
                format!(
                    "'{raw}' is not a connection version; `skardi handoff wait` prints the one to use"
                ),
            )
        })
}

fn check_alias(raw: &str) -> Result<String, AgentError> {
    let alias = raw.trim();
    if alias.is_empty() {
        return Err(AgentError::usage(
            "invalid_alias",
            "name the connection alias `skardi handoff wait` printed",
        ));
    }
    Ok(alias.to_string())
}

/// Spec §3.3's door, as `CheckReport::door_open` decides it: the credential
/// is alive, and every capability is verified or unverifiable, or refused in
/// a way the registry declares is a property of the account.
pub fn door_open(report: &Value) -> bool {
    let alive = report.get("credential").and_then(Value::as_str) == Some("alive");
    let verdicts_ok = report
        .get("verdicts")
        .and_then(Value::as_array)
        .is_some_and(|verdicts| {
            verdicts.iter().all(|v| {
                let verdict = v.get("verdict").and_then(Value::as_str).unwrap_or_default();
                matches!(verdict, "verified" | "unverifiable")
                    || (verdict == "refused"
                        && v.get("refusal_is_expected").and_then(Value::as_bool) == Some(true))
            })
        });
    alive && verdicts_ok
}

/// Row 4: configure-and-check, then the credential's liveness. One verdict
/// per capability, and whether the door lets the install proceed.
pub async fn permissions(
    control: &Control,
    alias: &str,
    version: &str,
) -> Result<Outcome, AgentError> {
    let alias = check_alias(alias)?;
    let version = check_version(version)?;
    let body = json!({ "connection_version_id": version });
    let at = encode_component(&alias);
    let report = control
        .post(
            &format!("/connections/preview/{at}/permissions"),
            Some(&body),
            &[],
        )
        .await?;
    let check = control
        .post(&format!("/connections/{at}/check"), Some(&body), &[])
        .await?;
    // A credential the provider calls dead closes the door whatever the
    // permission report says; `unavailable` (a provider with no
    // credential-only probe) is not a verdict on the credential and does not.
    let open = door_open(&report) && check.get("state").and_then(Value::as_str) != Some("invalid");
    let result = json!({ "permissions": report, "check": check, "door_open": open });
    let quoted = (shell_quote(&alias), shell_quote(&version));
    Ok(if open {
        Outcome::new(
            result,
            format!("{alias} may be installed: every capability it needs checked out"),
        )
        .showing_result()
        .with_next(format!(
            "skardi integrations candidates {} --connection-version {}",
            quoted.0, quoted.1
        ))
    } else {
        Outcome::new(
            result,
            format!(
                "{alias} cannot be installed yet: a capability is refused or the credential is not alive. Show the person the verdicts below; if they cannot fix it at the provider, cancel the handoff"
            ),
        )
        .showing_result()
        .with_next("skardi integrations cancel <handoff_id>")
    })
}

/// What `candidates` lists beyond the alias and version.
#[derive(Debug, Clone, Default)]
pub struct CandidateArgs {
    pub content_type: Option<String>,
    pub parent: Option<String>,
    pub cursor: Option<String>,
}

/// Row 5: one page of the picker, with the version it was listed against.
pub async fn candidates(
    control: &Control,
    alias: &str,
    version: &str,
    args: &CandidateArgs,
) -> Result<Outcome, AgentError> {
    let alias = check_alias(alias)?;
    let version = check_version(version)?;
    let mut query: Vec<(&str, &str)> = vec![("connection_version_id", version.as_str())];
    for (key, value) in [
        ("content_type", &args.content_type),
        ("parent", &args.parent),
        ("cursor", &args.cursor),
    ] {
        if let Some(value) = value.as_deref() {
            query.push((key, value));
        }
    }
    let page = control
        .get(
            &format!(
                "/connections/preview/{}/candidates",
                encode_component(&alias)
            ),
            &query,
        )
        .await?;
    // The page names the version it listed with; that is the one an install
    // must assert, so the hints carry it rather than the one asked for.
    let listed = page
        .get("connection_version_id")
        .and_then(Value::as_str)
        .unwrap_or(&version)
        .to_string();
    let rows = page
        .get("candidates")
        .and_then(Value::as_array)
        .map_or(0, Vec::len);
    let mut base = format!(
        "skardi integrations candidates {} --connection-version {}",
        shell_quote(&alias),
        shell_quote(&listed)
    );
    if let Some(content_type) = &args.content_type {
        base.push_str(&format!(" --content-type {}", shell_quote(content_type)));
    }
    if let Some(parent) = &args.parent {
        base.push_str(&format!(" --parent {}", shell_quote(parent)));
    }
    let next = match page.get("next_cursor").and_then(Value::as_str) {
        Some(cursor) if !cursor.is_empty() => format!("{base} --cursor {}", shell_quote(cursor)),
        _ => format!(
            "skardi integrations install {} --connection-version {} --scope '<json>'",
            shell_quote(&alias),
            shell_quote(&listed)
        ),
    };
    Ok(Outcome::new(
        page.clone(),
        format!(
            "{rows} candidate(s) for {alias}. Copy each chosen row's coordinate into --scope verbatim; confirm the scope with the person before installing"
        ),
    )
    .showing_result()
    .with_next(next))
}

/// Row 6: add to the scope by the server's merge, then poll the operation to
/// `done`.
///
/// `request_id` is the install's idempotency key. A fresh one per run, unless
/// given: re-running with the id an earlier run printed returns that
/// operation rather than submitting again, which is how a run that hit its
/// deadline picks up where it stopped.
///
/// The id is minted BEFORE the submit and named by every failure the install
/// may have outlived (see [`resumable`]): a submit whose answer was lost may
/// still have committed, and a run that then minted a fresh id would install
/// twice. `polling.deadline` bounds the whole command, submit included.
pub async fn install(
    control: &Control,
    alias: &str,
    version: &str,
    scope_raw: &str,
    request_id: Option<&str>,
    polling: Polling,
) -> Result<Outcome, AgentError> {
    let alias = check_alias(alias)?;
    let version = check_version(version)?;
    let scope = scope::parse(scope_raw)?;
    let request_id = match request_id {
        Some(id) if !id.trim().is_empty() => id.trim().to_string(),
        _ => uuid::Uuid::new_v4().to_string(),
    };
    let body = json!({
        "request_id": request_id,
        "connection_alias": alias,
        "connection_version_id": version,
        "scope": scope,
    });
    let retry = format!(
        "skardi integrations install {} --connection-version {} --scope {} --request-id {}",
        shell_quote(&alias),
        shell_quote(&version),
        shell_quote(scope_raw.trim()),
        shell_quote(&request_id)
    );
    let resume = |err: AgentError| resumable(err, &request_id, &retry);
    let still_running = |operation: Option<Value>| {
        let mut err = AgentError::local(
            EXIT_UNAVAILABLE,
            "install_pending",
            format!(
                "the install of {alias} is still running after {}s; run the same install again with its request id to keep waiting — it will not submit twice",
                polling.deadline.as_secs()
            ),
        )
        .with_field("request_id", json!(request_id))
        .with_next(retry.clone());
        if let Some(operation) = operation {
            err = err.with_field("operation", operation);
        }
        err
    };

    let deadline = polling.deadline_from_now();
    let submitted = within(
        deadline,
        control.post(
            "/installs",
            Some(&body),
            &[(SCOPE_FORMAT_HEADER, SCOPE_FORMAT)],
        ),
    )
    .await
    .ok_or_else(|| still_running(None))?
    .map_err(resume)?;
    let Some(operation_id) = submitted
        .get("operation_id")
        .and_then(Value::as_str)
        .map(str::to_string)
    else {
        return Err(resume(AgentError::local(
            EXIT_UNAVAILABLE,
            "invalid_response",
            "the control plane accepted the install but returned no operation id",
        )));
    };

    let path = format!("/installs/{}", encode_component(&operation_id));
    let mut interval = polling.first;
    let mut operation = submitted;
    loop {
        match operation.get("state").and_then(Value::as_str) {
            Some("done") => {
                let mut outcome = Outcome::new(
                    operation.clone(),
                    format!("installed {alias}: the scope was added, and its sources sync on the next tick"),
                )
                .with_next("skardi integrations status");
                if operation.get("link_owed").and_then(Value::as_bool) == Some(true) {
                    outcome = outcome.with_note(
                        "connected, with nothing to sync yet: new resources sync automatically when they appear",
                    );
                }
                return Ok(outcome);
            }
            Some("failed") => {
                let failure = operation
                    .get("failure")
                    .and_then(Value::as_str)
                    .unwrap_or("install_failed")
                    .to_string();
                return Err(AgentError::local(
                    EXIT_REFUSED,
                    &failure,
                    format!("the install of {alias} failed ({failure}); nothing was added"),
                )
                .with_field("operation", operation)
                .with_next(format!(
                    "skardi integrations permissions {} --connection-version {}",
                    shell_quote(&alias),
                    shell_quote(&version)
                )));
            }
            _ => {}
        }
        if Instant::now() + interval > deadline {
            return Err(still_running(Some(operation)));
        }
        sleep(interval).await;
        interval = polling.grow(interval);
        operation = match within(deadline, control.get(&path, &[])).await {
            Some(answer) => answer.map_err(resume)?,
            None => return Err(still_running(Some(operation))),
        };
    }
}

/// An install failure that does not say the install did not happen.
///
/// Exit 5 is "no answer, or a 5xx": a submit that timed out, lost its
/// connection, or met a `502` from the door may have committed upstream all
/// the same, and a poll that failed says nothing about the operation. Such a
/// failure carries the request id and, as `next`, the same install with
/// `--request-id`, which the server answers with the existing operation rather
/// than a second one. A refusal (exit 3, 6) is the server saying no, and is
/// left as it is.
fn resumable(err: AgentError, request_id: &str, retry: &str) -> AgentError {
    if err.exit != EXIT_UNAVAILABLE {
        return err;
    }
    let message = format!(
        "{} — the install may have been submitted all the same: run the same install again with --request-id {request_id} to pick it up; it will not submit twice",
        err.message()
    );
    let mut err = err
        .with_field("message", json!(message))
        .with_field("request_id", json!(request_id));
    err.next = Some(retry.to_string());
    err
}

/// Row 7: the person declined in chat, or `permissions` closed the door —
/// drop what the handoff staged. Never a disconnect: the server refuses it
/// once the version is active (`409 connection_in_use`).
pub async fn cancel(control: &Control, handoff: &str) -> Result<Outcome, AgentError> {
    let id = check_handoff_id(handoff)?;
    let view = control
        .post(&format!("/handoffs/{id}/abandon"), None, &[])
        .await?;
    let state = view
        .get("state")
        .and_then(Value::as_str)
        .unwrap_or("unknown")
        .to_string();
    Ok(Outcome::new(
        view,
        format!(
            "handoff {id} is {state}: whatever it staged was dropped, and nothing else changed"
        ),
    )
    .with_next("skardi integrations list"))
}

/// Row 8: one source's phase, health, runs and items; with none, a summary
/// of every source, or with `--detail` every SaaS and database source.
pub async fn status(
    control: &Control,
    source: Option<&str>,
    detail: bool,
) -> Result<Outcome, AgentError> {
    let Some(source) = source.map(str::trim).filter(|s| !s.is_empty()) else {
        let (path, what) = if detail {
            ("/sources", "every source")
        } else {
            ("/sources/summary", "a summary of every source")
        };
        let body = control.get(path, &[]).await?;
        return Ok(Outcome::new(body, format!("{what} in this workspace:")).showing_result());
    };
    let at = format!("/saas-sources/{}", encode_component(source));
    let view = control.get(&at, &[]).await?;
    let health = control.get(&format!("{at}/health"), &[]).await?;
    let runs = control.get(&format!("{at}/runs"), &[]).await?;
    // Items exist for a source narrowed to chosen ones; a source that has
    // none to name answers a refusal here, which is not a failure of status.
    let items = match control.get(&format!("{at}/items"), &[]).await {
        Ok(items) => items,
        Err(err) if err.exit == EXIT_REFUSED => Value::Null,
        Err(err) => return Err(err),
    };
    let no_runs = runs.as_array().is_some_and(Vec::is_empty);
    let result = json!({ "source": view, "health": health, "runs": runs, "items": items });
    let outcome = Outcome::new(
        result,
        if no_runs {
            format!("{source} has not run yet")
        } else {
            format!("{source}:")
        },
    )
    .showing_result();
    Ok(if no_runs {
        outcome.with_next(format!("skardi integrations sync {}", shell_quote(source)))
    } else {
        outcome
    })
}

/// Row 9: ask for one run now.
pub async fn sync(control: &Control, source: &str) -> Result<Outcome, AgentError> {
    let source = source.trim();
    if source.is_empty() {
        return Err(AgentError::usage(
            "invalid_source",
            "name the source, as `skardi integrations status` shows it",
        ));
    }
    let answer = control
        .post(
            &format!("/saas-sources/{}/sync", encode_component(source)),
            None,
            &[],
        )
        .await?;
    Ok(
        Outcome::new(answer, format!("a run of {source} was requested")).with_next(format!(
            "skardi integrations status {}",
            shell_quote(source)
        )),
    )
}

/// `Polling::INSTALL` with the deadline `--timeout` named.
pub fn install_polling(deadline: Option<Duration>) -> Polling {
    match deadline {
        Some(deadline) => Polling::INSTALL.with_deadline(deadline),
        None => Polling::INSTALL,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent::{EXIT_NO_CONTROL, EXIT_REFUSED, EXIT_UNAVAILABLE, EXIT_USAGE};
    use rstest::rstest;
    use wiremock::matchers::{body_json, header, method, path, query_param};
    use wiremock::{Mock, MockServer, Request, ResponseTemplate};

    const CONTROL: &str = "skardi_pat_control_secret";
    const WS: &str = "/control/v1/workspaces/acme-prod";
    const VERSION: &str = "0e6a3d2c-0000-4000-8000-000000000001";
    const HANDOFF: &str = "4b9f3c1e-8a2d-4c6b-9e0f-1a2b3c4d5e6f";

    fn control(server: &MockServer) -> Control {
        let mut control = Control::new(&server.uri(), "acme-prod", CONTROL).unwrap();
        control.retry_unit = Duration::from_millis(1);
        control
    }

    fn fast() -> Polling {
        Polling {
            first: Duration::from_millis(1),
            max: Duration::from_millis(2),
            deadline: Duration::from_millis(200),
        }
    }

    async fn answer(server: &MockServer, verb: &str, route: &str, status: u16, body: Value) {
        Mock::given(method(verb))
            .and(path(format!("{WS}{route}")))
            .and(header(
                "authorization",
                format!("Bearer {CONTROL}").as_str(),
            ))
            .respond_with(ResponseTemplate::new(status).set_body_json(body))
            .mount(server)
            .await;
    }

    fn requests_to<'a>(seen: &'a [Request], route: &str) -> Vec<&'a Request> {
        seen.iter()
            .filter(|r| r.url.path() == format!("{WS}{route}"))
            .collect()
    }

    #[tokio::test]
    async fn list_reads_the_four_routes_and_names_connect_next() {
        let server = MockServer::start().await;
        answer(
            &server,
            "GET",
            "",
            200,
            json!({"tenant_slug": "acme-prod", "provisioning_state": "active"}),
        )
        .await;
        answer(
            &server,
            "GET",
            "/integration-availability",
            200,
            json!({"integrations": [{"saas": "notion"}]}),
        )
        .await;
        answer(&server, "GET", "/connections", 200, json!([])).await;
        answer(&server, "GET", "/saas-sources", 200, json!([])).await;

        let outcome = list(&control(&server)).await.unwrap();
        assert_eq!(outcome.result["workspace"]["provisioning_state"], "active");
        assert_eq!(
            outcome.result["availability"]["integrations"][0]["saas"],
            "notion"
        );
        assert_eq!(outcome.next.as_deref(), Some(CONNECT_HINT));
        assert_eq!(server.received_requests().await.unwrap().len(), 4);
    }

    #[tokio::test]
    async fn list_on_a_provisioning_workspace_names_no_next_step() {
        let server = MockServer::start().await;
        answer(
            &server,
            "GET",
            "",
            200,
            json!({"tenant_slug": "acme-prod", "provisioning_state": "provisioning"}),
        )
        .await;
        for route in ["/integration-availability", "/connections", "/saas-sources"] {
            answer(&server, "GET", route, 200, json!([])).await;
        }
        let outcome = list(&control(&server)).await.unwrap();
        assert_eq!(outcome.next, None);
        assert!(outcome.notes[0].contains("provisioning"), "{outcome:?}");
    }

    #[rstest]
    #[case::new_target("new", json!({"saas": "notion", "target": {"kind": "new"}}))]
    #[case::reconnect("acme-notion", json!({"saas": "notion", "target": {"kind": "existing", "alias": "acme-notion"}}))]
    #[tokio::test]
    async fn connect_posts_the_target_and_names_handoff_wait(
        #[case] target: &str,
        #[case] expected: Value,
    ) {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path(format!("{WS}/handoffs")))
            .and(body_json(&expected))
            .respond_with(ResponseTemplate::new(201).set_body_json(json!({
                "handoff_id": HANDOFF,
                "kind": "provider_consent",
                "page_url": "https://console.example/w/acme-prod/handoff/4b9f",
                "expires_at": "2026-10-30T12:15:00Z",
            })))
            .expect(1)
            .mount(&server)
            .await;

        let outcome = connect(&control(&server), "notion", target).await.unwrap();
        assert_eq!(
            outcome.next.as_deref(),
            Some(format!("skardi handoff wait {HANDOFF}").as_str())
        );
        assert!(
            outcome
                .sentence
                .contains("https://console.example/w/acme-prod/handoff/4b9f")
        );
    }

    #[tokio::test]
    async fn connect_copies_a_servers_refusal_through_as_exit_3() {
        let server = MockServer::start().await;
        answer(&server, "POST", "/handoffs", 409, json!({"error": {"code": "target_exists", "message": "notion is connected as acme-notion", "alias": "acme-notion"}})).await;
        let err = connect(&control(&server), "notion", "new")
            .await
            .unwrap_err();
        assert_eq!(err.exit, EXIT_REFUSED);
        assert_eq!(err.code(), "target_exists");
        assert_eq!(err.error["alias"], "acme-notion");
    }

    fn report(credential: &str, verdicts: Value) -> Value {
        json!({"credential": credential, "verdicts": verdicts, "checked_at": "2026-10-30T12:00:00Z"})
    }

    #[rstest]
    #[case::all_verified(report("alive", json!([{"verdict": "verified"}, {"verdict": "unverifiable"}])), true)]
    #[case::expected_refusal(report("alive", json!([{"verdict": "refused", "refusal_is_expected": true}])), true)]
    #[case::refused(report("alive", json!([{"verdict": "refused"}])), false)]
    #[case::error(report("alive", json!([{"verdict": "error"}])), false)]
    #[case::dead_credential(report("invalid", json!([{"verdict": "verified"}])), false)]
    fn the_door_is_the_servers_rule(#[case] report: Value, #[case] open: bool) {
        assert_eq!(door_open(&report), open);
    }

    #[tokio::test]
    async fn permissions_sends_the_version_to_both_routes_and_names_candidates_next() {
        let server = MockServer::start().await;
        let body = json!({"connection_version_id": VERSION});
        Mock::given(method("POST"))
            .and(path(format!(
                "{WS}/connections/preview/acme-notion/permissions"
            )))
            .and(body_json(&body))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(report("alive", json!([{"verdict": "verified"}]))),
            )
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path(format!("{WS}/connections/acme-notion/check")))
            .and(body_json(&body))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(json!({"alias": "acme-notion", "state": "alive"})),
            )
            .expect(1)
            .mount(&server)
            .await;

        let outcome = permissions(&control(&server), "acme-notion", VERSION)
            .await
            .unwrap();
        assert_eq!(outcome.result["door_open"], true);
        assert_eq!(
            outcome.next.as_deref(),
            Some(
                format!(
                    "skardi integrations candidates acme-notion --connection-version {VERSION}"
                )
                .as_str()
            )
        );
    }

    #[tokio::test]
    async fn a_closed_door_names_cancel_next() {
        let server = MockServer::start().await;
        answer(
            &server,
            "POST",
            "/connections/preview/acme-notion/permissions",
            200,
            report("alive", json!([{"verdict": "refused"}])),
        )
        .await;
        answer(
            &server,
            "POST",
            "/connections/acme-notion/check",
            200,
            json!({"state": "alive"}),
        )
        .await;
        let outcome = permissions(&control(&server), "acme-notion", VERSION)
            .await
            .unwrap();
        assert_eq!(outcome.result["door_open"], false);
        assert_eq!(
            outcome.next.as_deref(),
            Some("skardi integrations cancel <handoff_id>")
        );
    }

    #[tokio::test]
    async fn a_version_that_is_not_a_uuid_issues_no_request() {
        let server = MockServer::start().await;
        let err = permissions(&control(&server), "acme-notion", "v1")
            .await
            .unwrap_err();
        assert_eq!(err.exit, EXIT_USAGE);
        assert!(server.received_requests().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn candidates_sends_the_query_and_pages_on_with_the_cursor() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(format!("{WS}/connections/preview/acme-github/candidates")))
            .and(query_param("connection_version_id", VERSION))
            .and(query_param("content_type", "issues"))
            .and(query_param("parent", "acme"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "connection_version_id": VERSION,
                "candidates": [{"id": "1", "coordinate": {"owner": "acme", "repo": "api"}, "label": "api"}],
                "next_cursor": "c 2",
            })))
            .expect(1)
            .mount(&server)
            .await;
        let args = CandidateArgs {
            content_type: Some("issues".to_string()),
            parent: Some("acme".to_string()),
            cursor: None,
        };
        let outcome = candidates(&control(&server), "acme-github", VERSION, &args)
            .await
            .unwrap();
        assert_eq!(
            outcome.next.as_deref(),
            Some(format!("skardi integrations candidates acme-github --connection-version {VERSION} --content-type issues --parent acme --cursor 'c 2'").as_str())
        );
    }

    #[tokio::test]
    async fn the_last_candidate_page_names_install_next() {
        let server = MockServer::start().await;
        answer(
            &server,
            "GET",
            "/connections/preview/acme-github/candidates",
            200,
            json!({"connection_version_id": VERSION, "candidates": []}),
        )
        .await;
        let outcome = candidates(
            &control(&server),
            "acme-github",
            VERSION,
            &CandidateArgs::default(),
        )
        .await
        .unwrap();
        assert_eq!(
            outcome.next.as_deref(),
            Some(format!("skardi integrations install acme-github --connection-version {VERSION} --scope '<json>'").as_str())
        );
    }

    #[tokio::test]
    async fn install_sends_the_format_header_the_version_and_the_scope_then_polls_to_done() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path(format!("{WS}/installs")))
            .and(header("x-skardi-scope-format", "content-types-v1"))
            .respond_with(
                ResponseTemplate::new(202)
                    .set_body_json(json!({"operation_id": "op-1", "state": "pending"})),
            )
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path(format!("{WS}/installs/op-1")))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(json!({"operation_id": "op-1", "state": "pending"})),
            )
            .up_to_n_times(1)
            .mount(&server)
            .await;
        answer(
            &server,
            "GET",
            "/installs/op-1",
            200,
            json!({"operation_id": "op-1", "state": "done", "link_owed": false}),
        )
        .await;

        let scope = r#"{"issues":[{"kind":"leaf","coordinate":{"owner":"acme","repo":"api"}}],"pull_requests":[]}"#;
        let outcome = install(
            &control(&server),
            "acme-github",
            VERSION,
            scope,
            None,
            fast(),
        )
        .await
        .unwrap();
        assert_eq!(outcome.result["state"], "done");
        assert_eq!(outcome.next.as_deref(), Some("skardi integrations status"));

        let seen = server.received_requests().await.unwrap();
        let posted = requests_to(&seen, "/installs");
        let body: Value = serde_json::from_slice(&posted[0].body).unwrap();
        assert_eq!(body["connection_alias"], "acme-github");
        assert_eq!(body["connection_version_id"], VERSION);
        assert_eq!(body["scope"], serde_json::from_str::<Value>(scope).unwrap());
        assert!(uuid::Uuid::parse_str(body["request_id"].as_str().unwrap()).is_ok());
        assert_eq!(requests_to(&seen, "/installs/op-1").len(), 2);
    }

    #[tokio::test]
    async fn an_exclude_is_refused_before_any_request() {
        let server = MockServer::start().await;
        let err = install(
            &control(&server),
            "acme-notion",
            VERSION,
            r#"{"pages":[{"kind":"exclude","coordinate":{"page_id":"p"}}]}"#,
            None,
            fast(),
        )
        .await
        .unwrap_err();
        assert_eq!(err.code(), "scope_exclude_not_agent_callable");
        assert_eq!(err.exit, EXIT_USAGE);
        assert!(server.received_requests().await.unwrap().is_empty());
    }

    #[rstest]
    #[case::account_changed(409, "connection_account_changed")]
    #[case::version_changed(409, "connection_version_changed")]
    #[case::would_remove(409, "scope_would_remove")]
    #[case::incomplete(400, "incomplete_scope")]
    #[tokio::test]
    async fn install_refusals_are_copied_through(#[case] status: u16, #[case] code: &str) {
        let server = MockServer::start().await;
        answer(
            &server,
            "POST",
            "/installs",
            status,
            json!({"error": {"code": code, "message": "refused"}}),
        )
        .await;
        let err = install(
            &control(&server),
            "acme-github",
            VERSION,
            r#"{"issues":null}"#,
            None,
            fast(),
        )
        .await
        .unwrap_err();
        assert_eq!(err.code(), code);
        assert_eq!(err.exit, EXIT_REFUSED);
    }

    #[tokio::test]
    async fn a_failed_operation_is_exit_3_with_its_failure() {
        let server = MockServer::start().await;
        answer(
            &server,
            "POST",
            "/installs",
            202,
            json!({"operation_id": "op-1", "state": "failed", "failure": "permissions_incomplete"}),
        )
        .await;
        let err = install(
            &control(&server),
            "acme-github",
            VERSION,
            r#"{"issues":null}"#,
            None,
            fast(),
        )
        .await
        .unwrap_err();
        assert_eq!(err.exit, EXIT_REFUSED);
        assert_eq!(err.code(), "permissions_incomplete");
    }

    /// The deadline names the same install with its request id, which the
    /// server answers with the existing operation rather than a second one.
    #[tokio::test]
    async fn a_pending_install_at_the_deadline_names_the_idempotent_rerun() {
        let server = MockServer::start().await;
        answer(
            &server,
            "POST",
            "/installs",
            202,
            json!({"operation_id": "op-1", "state": "pending"}),
        )
        .await;
        answer(
            &server,
            "GET",
            "/installs/op-1",
            200,
            json!({"operation_id": "op-1", "state": "pending"}),
        )
        .await;
        let err = install(
            &control(&server),
            "acme-github",
            VERSION,
            r#"{"issues":null}"#,
            Some("req-7"),
            fast(),
        )
        .await
        .unwrap_err();
        assert_eq!(err.exit, EXIT_UNAVAILABLE);
        assert_eq!(err.code(), "install_pending");
        assert_eq!(
            err.next.as_deref(),
            Some(format!("skardi integrations install acme-github --connection-version {VERSION} --scope '{{\"issues\":null}}' --request-id req-7").as_str())
        );
        let seen = server.received_requests().await.unwrap();
        let body: Value = serde_json::from_slice(&requests_to(&seen, "/installs")[0].body).unwrap();
        assert_eq!(body["request_id"], "req-7");
    }

    /// The request id the install POST carried.
    async fn posted_request_id(server: &MockServer) -> String {
        let seen = server.received_requests().await.unwrap();
        let body: Value = serde_json::from_slice(&requests_to(&seen, "/installs")[0].body).unwrap();
        body["request_id"].as_str().unwrap().to_string()
    }

    fn replay(request_id: &str) -> String {
        format!(
            "skardi integrations install acme-github --connection-version {VERSION} --scope '{{\"issues\":null}}' --request-id {request_id}"
        )
    }

    /// Finding: a submit whose answer is lost may have committed. The error
    /// names the id this run generated, and the run that resumes it, so the
    /// retry is the same install rather than a second one.
    #[rstest]
    #[case::door_503(503, json!({"error": {"code": "control_plane_unavailable", "message": "down"}}), "control_plane_unavailable")]
    #[case::proxy_502(502, json!("bad gateway"), "http_502")]
    #[tokio::test]
    async fn a_5xx_submit_names_its_request_id_and_the_replay(
        #[case] status: u16,
        #[case] body: Value,
        #[case] code: &str,
    ) {
        let server = MockServer::start().await;
        answer(&server, "POST", "/installs", status, body).await;
        let err = install(
            &control(&server),
            "acme-github",
            VERSION,
            r#"{"issues":null}"#,
            None,
            fast(),
        )
        .await
        .unwrap_err();
        let id = posted_request_id(&server).await;
        assert_eq!(err.exit, EXIT_UNAVAILABLE);
        assert_eq!(err.code(), code);
        assert_eq!(err.error["request_id"], id.as_str());
        assert_eq!(err.next.as_deref(), Some(replay(&id).as_str()));
        assert!(err.message().contains("--request-id"), "{err:?}");
    }

    /// A submit that outlives `--timeout` is cut there, and is just as
    /// uncertain as a lost answer: it names the id and the replay.
    #[tokio::test]
    async fn a_submit_that_outlives_the_timeout_names_its_request_id_and_the_replay() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path(format!("{WS}/installs")))
            .respond_with(
                ResponseTemplate::new(202)
                    .set_body_json(json!({"operation_id": "op-1", "state": "pending"}))
                    .set_delay(Duration::from_secs(30)),
            )
            .mount(&server)
            .await;
        let started = std::time::Instant::now();
        let err = install(
            &control(&server),
            "acme-github",
            VERSION,
            r#"{"issues":null}"#,
            None,
            fast(),
        )
        .await
        .unwrap_err();
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "took {:?}",
            started.elapsed()
        );
        let id = posted_request_id(&server).await;
        assert_eq!(err.exit, EXIT_UNAVAILABLE);
        assert_eq!(err.code(), "install_pending");
        assert_eq!(err.error["request_id"], id.as_str());
        assert_eq!(err.next.as_deref(), Some(replay(&id).as_str()));
    }

    /// No gateway at all: the same hint, since a connection that failed
    /// mid-request may still have delivered it.
    #[tokio::test]
    async fn an_unreachable_submit_names_its_request_id_and_the_replay() {
        let control = Control::new("http://127.0.0.1:1", "acme-prod", CONTROL).unwrap();
        // A long deadline: Windows takes about two seconds to refuse a
        // loopback connection, which a short one would cut off first.
        let err = install(
            &control,
            "acme-github",
            VERSION,
            r#"{"issues":null}"#,
            Some("req-9"),
            fast().with_deadline(Duration::from_secs(60)),
        )
        .await
        .unwrap_err();
        assert_eq!(err.code(), "control_plane_unreachable");
        assert_eq!(err.error["request_id"], "req-9");
        assert_eq!(err.next.as_deref(), Some(replay("req-9").as_str()));
        assert!(!err.message().contains(CONTROL), "{err:?}");
    }

    /// A poll that stalls past the deadline, or fails, after a submit that
    /// committed: the operation is running, and the replay picks it up.
    #[rstest]
    #[case::stalled(ResponseTemplate::new(200).set_body_json(json!({"operation_id": "op-1", "state": "pending"})).set_delay(Duration::from_secs(30)), "install_pending")]
    #[case::failed(ResponseTemplate::new(503).set_body_json(json!({"error": {"code": "control_plane_unavailable", "message": "down"}})), "control_plane_unavailable")]
    #[tokio::test]
    async fn a_poll_that_fails_after_the_submit_names_the_replay(
        #[case] poll: ResponseTemplate,
        #[case] code: &str,
    ) {
        let server = MockServer::start().await;
        answer(
            &server,
            "POST",
            "/installs",
            202,
            json!({"operation_id": "op-1", "state": "pending"}),
        )
        .await;
        Mock::given(method("GET"))
            .and(path(format!("{WS}/installs/op-1")))
            .respond_with(poll)
            .mount(&server)
            .await;
        let started = std::time::Instant::now();
        let err = install(
            &control(&server),
            "acme-github",
            VERSION,
            r#"{"issues":null}"#,
            Some("req-7"),
            fast(),
        )
        .await
        .unwrap_err();
        assert!(started.elapsed() < Duration::from_secs(5));
        assert_eq!(err.code(), code);
        assert_eq!(err.exit, EXIT_UNAVAILABLE);
        assert_eq!(err.error["request_id"], "req-7");
        assert_eq!(err.next.as_deref(), Some(replay("req-7").as_str()));
    }

    /// A refusal is the server saying no, and is not dressed as resumable.
    #[tokio::test]
    async fn a_refused_submit_names_no_replay() {
        let server = MockServer::start().await;
        answer(
            &server,
            "POST",
            "/installs",
            409,
            json!({"error": {"code": "scope_would_remove", "message": "no"}}),
        )
        .await;
        let err = install(
            &control(&server),
            "acme-github",
            VERSION,
            r#"{"issues":null}"#,
            None,
            fast(),
        )
        .await
        .unwrap_err();
        assert_eq!(err.next, None);
        assert!(err.error.get("request_id").is_none(), "{err:?}");
    }

    #[tokio::test]
    async fn an_accepted_install_without_an_operation_id_names_the_replay() {
        let server = MockServer::start().await;
        answer(
            &server,
            "POST",
            "/installs",
            202,
            json!({"state": "pending"}),
        )
        .await;
        let err = install(
            &control(&server),
            "acme-github",
            VERSION,
            r#"{"issues":null}"#,
            Some("req-3"),
            fast(),
        )
        .await
        .unwrap_err();
        assert_eq!(err.code(), "invalid_response");
        assert_eq!(err.next.as_deref(), Some(replay("req-3").as_str()));
    }

    /// A deadline shorter than the next interval ends the install at once
    /// rather than sleeping past it, still naming the replay.
    #[tokio::test]
    async fn a_deadline_shorter_than_the_next_interval_is_not_slept_through() {
        let server = MockServer::start().await;
        answer(
            &server,
            "POST",
            "/installs",
            202,
            json!({"operation_id": "op-1", "state": "pending"}),
        )
        .await;
        let polling = Polling {
            first: Duration::from_secs(30),
            max: Duration::from_secs(30),
            deadline: Duration::from_millis(200),
        };
        let started = std::time::Instant::now();
        let err = install(
            &control(&server),
            "acme-github",
            VERSION,
            r#"{"issues":null}"#,
            Some("req-1"),
            polling,
        )
        .await
        .unwrap_err();
        assert!(started.elapsed() < Duration::from_secs(5));
        assert_eq!(err.code(), "install_pending");
        assert_eq!(err.error["operation"]["state"], "pending");
        assert_eq!(err.next.as_deref(), Some(replay("req-1").as_str()));
    }

    #[test]
    fn install_polling_takes_the_timeout_or_the_ten_minute_default() {
        assert_eq!(install_polling(None), Polling::INSTALL);
        assert_eq!(
            install_polling(Some(Duration::from_secs(5))).deadline,
            Duration::from_secs(5)
        );
        assert_eq!(Polling::INSTALL.deadline, Duration::from_secs(600));
    }

    /// An install with nothing to sync yet says so, beside the success.
    #[tokio::test]
    async fn a_done_install_with_a_link_owed_notes_it() {
        let server = MockServer::start().await;
        answer(
            &server,
            "POST",
            "/installs",
            202,
            json!({"operation_id": "op-1", "state": "done", "link_owed": true}),
        )
        .await;
        let outcome = install(
            &control(&server),
            "acme-github",
            VERSION,
            r#"{"issues":null}"#,
            None,
            fast(),
        )
        .await
        .unwrap();
        assert!(
            outcome.notes[0].contains("nothing to sync yet"),
            "{outcome:?}"
        );
    }

    /// The handoff page asks for different things by kind; the sentence
    /// tells the agent which to relay.
    #[rstest]
    #[case::credential("secret_drop_credential", "paste the token on that page")]
    #[case::oauth_client("secret_drop_oauth_client", "create an app at the provider")]
    #[case::consent("provider_consent", "consent at the provider")]
    #[tokio::test]
    async fn connect_says_what_the_page_asks_for(#[case] kind: &str, #[case] says: &str) {
        let server = MockServer::start().await;
        answer(
            &server,
            "POST",
            "/handoffs",
            201,
            json!({"handoff_id": HANDOFF, "kind": kind, "page_url": "https://console.example/h"}),
        )
        .await;
        let outcome = connect(&control(&server), "github", "new").await.unwrap();
        assert!(outcome.sentence.contains(says), "{}", outcome.sentence);
        // No `expires_at`, no dangling clause about it.
        assert!(
            !outcome.sentence.contains("works until"),
            "{}",
            outcome.sentence
        );
    }

    #[tokio::test]
    async fn a_handoff_without_its_link_is_an_invalid_response() {
        let server = MockServer::start().await;
        answer(
            &server,
            "POST",
            "/handoffs",
            201,
            json!({"handoff_id": HANDOFF}),
        )
        .await;
        let err = connect(&control(&server), "notion", "new")
            .await
            .unwrap_err();
        assert_eq!(err.code(), "invalid_response");
        assert_eq!(err.exit, EXIT_UNAVAILABLE);
    }

    /// Blank arguments are usage errors, caught before a request.
    #[rstest]
    #[case::saas("  ", "new", "invalid_saas")]
    #[case::target("notion", " ", "target_required")]
    #[tokio::test]
    async fn connect_refuses_blank_arguments_locally(
        #[case] saas: &str,
        #[case] target: &str,
        #[case] code: &str,
    ) {
        let server = MockServer::start().await;
        let err = connect(&control(&server), saas, target).await.unwrap_err();
        assert_eq!(err.code(), code);
        assert_eq!(err.exit, EXIT_USAGE);
        assert!(server.received_requests().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn a_blank_alias_or_source_is_a_usage_error() {
        let server = MockServer::start().await;
        let err = permissions(&control(&server), " ", VERSION)
            .await
            .unwrap_err();
        assert_eq!(err.code(), "invalid_alias");
        let err = sync(&control(&server), " ").await.unwrap_err();
        assert_eq!(err.code(), "invalid_source");
        assert!(server.received_requests().await.unwrap().is_empty());
    }

    /// `items` answering a refusal is a source with none to name; answering
    /// a 5xx is a failure of `status`.
    #[tokio::test]
    async fn status_fails_when_items_fail_with_the_control_plane() {
        let server = MockServer::start().await;
        for (route, body) in [
            (
                "/saas-sources/acme-notion",
                json!({"source_name": "acme-notion"}),
            ),
            (
                "/saas-sources/acme-notion/health",
                json!({"phase": "active"}),
            ),
            ("/saas-sources/acme-notion/runs", json!([{"run_id": "r1"}])),
        ] {
            answer(&server, "GET", route, 200, body).await;
        }
        answer(
            &server,
            "GET",
            "/saas-sources/acme-notion/items",
            503,
            json!({"error": {"code": "control_plane_unavailable", "message": "down"}}),
        )
        .await;
        let err = status(&control(&server), Some("acme-notion"), false)
            .await
            .unwrap_err();
        assert_eq!(err.exit, EXIT_UNAVAILABLE);
    }

    /// A source that has run names no next step.
    #[tokio::test]
    async fn status_of_a_source_that_ran_names_no_next_step() {
        let server = MockServer::start().await;
        for (route, body) in [
            (
                "/saas-sources/acme-notion",
                json!({"source_name": "acme-notion"}),
            ),
            (
                "/saas-sources/acme-notion/health",
                json!({"phase": "active"}),
            ),
            ("/saas-sources/acme-notion/runs", json!([{"run_id": "r1"}])),
            ("/saas-sources/acme-notion/items", json!([])),
        ] {
            answer(&server, "GET", route, 200, body).await;
        }
        let outcome = status(&control(&server), Some("acme-notion"), false)
            .await
            .unwrap();
        assert_eq!(outcome.next, None);
        assert_eq!(outcome.sentence, "acme-notion:");
    }

    #[tokio::test]
    async fn cancel_abandons_the_handoff() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path(format!("{WS}/handoffs/{HANDOFF}/abandon")))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"state": "abandoned", "kind": "provider_consent", "expires_at": "2026-10-30T12:30:00Z"})))
            .expect(1)
            .mount(&server)
            .await;
        let outcome = cancel(&control(&server), HANDOFF).await.unwrap();
        assert_eq!(outcome.result["state"], "abandoned");
        assert_eq!(outcome.next.as_deref(), Some("skardi integrations list"));
    }

    #[tokio::test]
    async fn cancel_after_activation_is_the_servers_refusal() {
        let server = MockServer::start().await;
        answer(
            &server,
            "POST",
            &format!("/handoffs/{HANDOFF}/abandon"),
            409,
            json!({"error": {"code": "connection_in_use", "message": "active"}}),
        )
        .await;
        let err = cancel(&control(&server), HANDOFF).await.unwrap_err();
        assert_eq!(err.code(), "connection_in_use");
        assert_eq!(err.exit, EXIT_REFUSED);
    }

    #[tokio::test]
    async fn status_of_one_source_reads_its_four_routes_and_names_sync_when_it_never_ran() {
        let server = MockServer::start().await;
        answer(
            &server,
            "GET",
            "/saas-sources/acme-notion",
            200,
            json!({"source_name": "acme-notion"}),
        )
        .await;
        answer(
            &server,
            "GET",
            "/saas-sources/acme-notion/health",
            200,
            json!({"phase": "active"}),
        )
        .await;
        answer(
            &server,
            "GET",
            "/saas-sources/acme-notion/runs",
            200,
            json!([]),
        )
        .await;
        answer(
            &server,
            "GET",
            "/saas-sources/acme-notion/items",
            404,
            json!({"error": {"code": "not_found", "message": "none"}}),
        )
        .await;
        let outcome = status(&control(&server), Some("acme-notion"), false)
            .await
            .unwrap();
        assert_eq!(outcome.result["health"]["phase"], "active");
        assert_eq!(outcome.result["items"], Value::Null);
        assert_eq!(
            outcome.next.as_deref(),
            Some("skardi integrations sync acme-notion")
        );
    }

    #[rstest]
    #[case::summary(false, "/sources/summary")]
    #[case::detail(true, "/sources")]
    #[tokio::test]
    async fn status_without_a_source_reads_the_summary_or_the_detail(
        #[case] detail: bool,
        #[case] route: &str,
    ) {
        let server = MockServer::start().await;
        answer(&server, "GET", route, 200, json!({"sources": []})).await;
        status(&control(&server), None, detail).await.unwrap();
        let seen = server.received_requests().await.unwrap();
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0].url.path(), format!("{WS}{route}"));
    }

    #[tokio::test]
    async fn sync_posts_and_names_status_next() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path(format!("{WS}/saas-sources/acme-notion/sync")))
            .respond_with(ResponseTemplate::new(202).set_body_json(json!({"requested": true})))
            .expect(1)
            .mount(&server)
            .await;
        let outcome = sync(&control(&server), "acme-notion").await.unwrap();
        assert_eq!(
            outcome.next.as_deref(),
            Some("skardi integrations status acme-notion")
        );
    }

    #[tokio::test]
    async fn an_expired_control_token_at_the_server_is_exit_6() {
        let server = MockServer::start().await;
        answer(&server, "POST", "/saas-sources/acme-notion/sync", 401, json!({"error": {"code": "token_unknown_or_revoked", "message": "run skardi login --control"}})).await;
        let err = sync(&control(&server), "acme-notion").await.unwrap_err();
        assert_eq!(err.exit, EXIT_NO_CONTROL);
        assert_eq!(err.next.as_deref(), Some("skardi login --control"));
    }
}
