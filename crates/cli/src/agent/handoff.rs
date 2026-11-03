//! `skardi handoff wait <id>` (design 2026-10-27 §2.6, §2.4.2): wait for the
//! person to finish what only they can do — a consent at the provider, or a
//! secret typed into the handoff page — and print the connection it produced.
//!
//! The wait is a pure read of `GET …/handoffs/:handoff_id` (§2.6.3: the
//! agent's poll never completes anything). Its token view carries the state,
//! and once `completed` the alias and version that `permissions`,
//! `candidates` and `install` then name.
//!
//! | State | Here |
//! | --- | --- |
//! | `open`, `opened` | keep polling; at the deadline, `still_open`, exit 4 |
//! | `completed` | the alias and version, exit 0 |
//! | `expired`, `denied`, `abandoned` | named, exit 4 |

use super::control::Control;
use super::{AgentError, EXIT_HANDOFF_ENDED, EXIT_REFUSED, EXIT_UNAVAILABLE, EXIT_USAGE, Outcome};
use serde_json::{Value, json};
use std::future::Future;
use std::time::Duration;
use tokio::time::{Instant, sleep, timeout_at};

/// How a wait paces itself and when it gives up.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Polling {
    /// The first interval.
    pub first: Duration,
    /// The interval stops growing here.
    pub max: Duration,
    /// When to stop waiting.
    pub deadline: Duration,
}

impl Polling {
    /// §2.4.2: every 2 seconds, growing by half up to 10 seconds; 30 minutes,
    /// which is a handoff's own lifetime after it is first opened (§2.6.6).
    pub const HANDOFF: Polling = Polling {
        first: Duration::from_secs(2),
        max: Duration::from_secs(10),
        deadline: Duration::from_secs(30 * 60),
    };

    /// An install's operation: the same pace, and ten minutes — the
    /// operation's steps are server-side work, not a person in a browser.
    pub const INSTALL: Polling = Polling {
        first: Duration::from_secs(2),
        max: Duration::from_secs(10),
        deadline: Duration::from_secs(10 * 60),
    };

    /// The interval after `interval`: half again, capped.
    pub fn grow(&self, interval: Duration) -> Duration {
        (interval + interval / 2).min(self.max)
    }

    pub fn with_deadline(mut self, deadline: Duration) -> Polling {
        self.deadline = deadline;
        self
    }

    /// The instant a wait starting now must end by.
    ///
    /// `--timeout` takes any number of hours, and `Instant + Duration` panics
    /// past the clock's range, so a deadline too far out to represent is
    /// clamped to one that is merely very far out.
    pub fn deadline_from_now(&self) -> Instant {
        let now = Instant::now();
        now.checked_add(self.deadline)
            .unwrap_or_else(|| now + FAR_FUTURE)
    }
}

/// What an unrepresentable `--timeout` is clamped to: a year, which no
/// handoff or install outlives.
const FAR_FUTURE: Duration = Duration::from_secs(365 * 24 * 60 * 60);

/// Run one request of a wait within what is left of its budget: `None` when
/// the deadline came first, and the request is dropped mid-flight.
///
/// The deadline is checked between requests too, but that alone does not
/// bound the command: one `GET` to a stalled gateway can hold for the control
/// client's whole 130-second timeout, and a `429`'s `Retry-After` is slept
/// INSIDE the request. Wrapping the request covers both, so `--timeout 5s`
/// means five seconds.
pub async fn within<F: Future>(deadline: Instant, request: F) -> Option<F::Output> {
    timeout_at(deadline, request).await.ok()
}

/// Consecutive transport failures a wait rides out before reporting the
/// control plane as unreachable — a gateway rolling mid-wait should not end a
/// wait the person is still working through.
const MAX_TRANSPORT_FAILURES: u32 = 3;

/// How long a landing nothing activates lives before it is reclaimed
/// (`UNACTIVATED_LANDING_TTL`, §2.6.5). Printed after a completed handoff,
/// because a scope question left longer loses the landing and the install
/// then answers `connection_not_found`.
const LANDING_TTL_MINUTES: i64 = 60;

/// Parse `--timeout`: `30m`, `90s`, `1h`, or a bare number of seconds.
pub fn parse_timeout(raw: &str) -> Result<Duration, AgentError> {
    let trimmed = raw.trim();
    let (digits, unit) = match trimmed.char_indices().last() {
        Some((at, c)) if c.is_ascii_alphabetic() => (&trimmed[..at], c.to_ascii_lowercase()),
        _ => (trimmed, 's'),
    };
    let amount: u64 = digits.trim().parse().map_err(|_| {
        AgentError::usage(
            "invalid_timeout",
            format!(
                "--timeout takes seconds, minutes or hours, like '90s', '30m' or '1h', not '{raw}'"
            ),
        )
    })?;
    let seconds = match unit {
        's' => Some(amount),
        'm' => amount.checked_mul(60),
        'h' => amount.checked_mul(3600),
        _ => None,
    };
    match seconds {
        Some(seconds) if seconds > 0 => Ok(Duration::from_secs(seconds)),
        _ => Err(AgentError::usage(
            "invalid_timeout",
            format!(
                "--timeout must be a positive number of seconds, minutes or hours, not '{raw}'"
            ),
        )),
    }
}

/// A handoff id is a UUID; anything else is a typo worth naming before it
/// becomes a `404` that reads like a vanished handoff.
pub fn check_handoff_id(raw: &str) -> Result<String, AgentError> {
    let trimmed = raw.trim();
    uuid::Uuid::parse_str(trimmed)
        .map(|id| id.to_string())
        .map_err(|_| {
            AgentError::local(
                EXIT_USAGE,
                "invalid_handoff_id",
                format!("'{raw}' is not a handoff id; `skardi integrations connect` prints one"),
            )
        })
}

/// Wait on one handoff.
pub async fn wait(
    control: &Control,
    raw_id: &str,
    polling: Polling,
) -> Result<Outcome, AgentError> {
    let id = check_handoff_id(raw_id)?;
    let path = format!("/handoffs/{id}");
    let deadline = polling.deadline_from_now();
    let mut interval = polling.first;
    let mut transport_failures = 0;
    let mut last: Option<Value> = None;
    loop {
        let Some(answer) = within(deadline, control.get(&path, &[])).await else {
            return Err(still_open(&id, last, polling.deadline));
        };
        match answer {
            Ok(view) => {
                transport_failures = 0;
                let state = view
                    .get("state")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string();
                match state.as_str() {
                    "completed" => return completed(view),
                    "open" | "opened" => last = Some(view),
                    "expired" | "denied" | "abandoned" => return Err(ended(&state, view)),
                    other => {
                        return Err(AgentError::local(
                            EXIT_REFUSED,
                            "unexpected_handoff_state",
                            format!(
                                "the handoff is in a state this skardi CLI does not know ({other:?}) — it may be older than the control plane"
                            ),
                        )
                        .with_field("handoff", view));
                    }
                }
            }
            Err(err)
                if err.code() == "control_plane_unreachable"
                    && transport_failures < MAX_TRANSPORT_FAILURES =>
            {
                transport_failures += 1;
            }
            Err(err) => return Err(err),
        }
        if Instant::now() + interval > deadline {
            return Err(still_open(&id, last, polling.deadline));
        }
        sleep(interval).await;
        interval = polling.grow(interval);
    }
}

fn completed(view: Value) -> Result<Outcome, AgentError> {
    let alias = view.get("connection_alias").and_then(Value::as_str);
    let version = view.get("connection_version_id").and_then(Value::as_str);
    let (Some(alias), Some(version)) = (alias, version) else {
        return Err(AgentError::local(
            EXIT_UNAVAILABLE,
            "invalid_response",
            "the handoff is completed but names no connection alias and version",
        )
        .with_field("handoff", view));
    };
    let reclaim = chrono::Utc::now() + chrono::Duration::minutes(LANDING_TTL_MINUTES);
    let next = format!(
        "skardi integrations permissions {} --connection-version {}",
        super::shell_quote(alias),
        super::shell_quote(version)
    );
    Ok(Outcome::new(
        view.clone(),
        format!("connected: {alias}, version {version}"),
    )
    .with_next(next)
    .with_note(format!(
        "install within the hour: a connection nothing installs is reclaimed about {}, and the install then answers connection_not_found",
        reclaim.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
    )))
}

fn ended(state: &str, view: Value) -> AgentError {
    let (message, next) = match state {
        "expired" => (
            "the handoff expired before the person finished on its page (15 minutes unopened, 30 after opening). Nothing was connected; open a new one",
            Some("skardi integrations connect <saas> --target <new|alias>"),
        ),
        "denied" => (
            "the person declined on the handoff page. Nothing was connected; ask them in chat before trying again",
            None,
        ),
        _ => (
            "the handoff was cancelled with `skardi integrations cancel`. Nothing was installed",
            None,
        ),
    };
    let mut err = AgentError::local(EXIT_HANDOFF_ENDED, state, message).with_field("handoff", view);
    err.next = next.map(str::to_string);
    err
}

fn still_open(id: &str, last: Option<Value>, deadline: Duration) -> AgentError {
    let until = last
        .as_ref()
        .and_then(|view| view.get("expires_at"))
        .and_then(Value::as_str)
        .map(|at| format!("; the link stays usable until {at}"))
        .unwrap_or_default();
    // No answer at all within the budget says nothing about the person: the
    // control plane was too slow to ask. Still `still_open`, the deadline's
    // one outcome, but not worded as though the handoff had been read.
    let message = if last.is_some() {
        format!(
            "the person has not finished on the handoff page after {}s{until}. Ask whether they are still working on it, then wait again",
            deadline.as_secs()
        )
    } else {
        format!(
            "the control plane did not say how the handoff stands within {}s; nothing ended it, so wait again",
            deadline.as_secs()
        )
    };
    AgentError::local(EXIT_HANDOFF_ENDED, "still_open", message)
        .with_field("handoff", last.unwrap_or(json!(null)))
        .with_next(format!("skardi handoff wait {id}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent::{EXIT_HANDOFF_ENDED, EXIT_REFUSED, EXIT_UNAVAILABLE, EXIT_USAGE};
    use rstest::rstest;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    const ID: &str = "4b9f3c1e-8a2d-4c6b-9e0f-1a2b3c4d5e6f";

    fn fast() -> Polling {
        Polling {
            first: Duration::from_millis(1),
            max: Duration::from_millis(2),
            deadline: Duration::from_millis(200),
        }
    }

    async fn gateway_answering(states: &[Value]) -> MockServer {
        let server = MockServer::start().await;
        let route = format!("/control/v1/workspaces/acme-prod/handoffs/{ID}");
        let (last, first) = states.split_last().unwrap();
        for view in first {
            Mock::given(method("GET"))
                .and(path(route.as_str()))
                .respond_with(ResponseTemplate::new(200).set_body_json(view))
                .up_to_n_times(1)
                .mount(&server)
                .await;
        }
        Mock::given(method("GET"))
            .and(path(route.as_str()))
            .respond_with(ResponseTemplate::new(200).set_body_json(last))
            .mount(&server)
            .await;
        server
    }

    fn view(state: &str) -> Value {
        json!({"state": state, "kind": "provider_consent", "expires_at": "2026-10-30T12:30:00Z"})
    }

    fn control(server: &MockServer) -> Control {
        Control::new(&server.uri(), "acme-prod", "skardi_pat_ctl").unwrap()
    }

    #[tokio::test]
    async fn a_completed_handoff_names_the_alias_and_version_and_the_permissions_step() {
        let mut done = view("completed");
        done["connection_alias"] = json!("acme-notion");
        done["connection_version_id"] = json!("0e6a3d2c-0000-4000-8000-000000000001");
        let server = gateway_answering(&[view("open"), view("opened"), done]).await;

        let outcome = wait(&control(&server), ID, fast()).await.unwrap();
        assert_eq!(outcome.result["connection_alias"], "acme-notion");
        assert_eq!(
            outcome.next.as_deref(),
            Some(
                "skardi integrations permissions acme-notion --connection-version 0e6a3d2c-0000-4000-8000-000000000001"
            )
        );
        assert!(outcome.notes[0].contains("within the hour"), "{outcome:?}");
        // It polled until done, and only read.
        let seen = server.received_requests().await.unwrap();
        assert_eq!(seen.len(), 3);
        assert!(seen.iter().all(|r| r.method == wiremock::http::Method::GET));
    }

    /// `still_open`, `expired`, `denied` and `abandoned` are four different
    /// answers on one exit code, told apart by the error code.
    #[rstest]
    #[case::expired(
        "expired",
        Some("skardi integrations connect <saas> --target <new|alias>")
    )]
    #[case::denied("denied", None)]
    #[case::abandoned("abandoned", None)]
    #[tokio::test]
    async fn an_ended_handoff_is_exit_4_naming_its_state(
        #[case] state: &str,
        #[case] next: Option<&str>,
    ) {
        let server = gateway_answering(&[view("opened"), view(state)]).await;
        let err = wait(&control(&server), ID, fast()).await.unwrap_err();
        assert_eq!(err.exit, EXIT_HANDOFF_ENDED);
        assert_eq!(err.code(), state);
        assert_eq!(err.next.as_deref(), next);
        assert_eq!(err.error["handoff"]["state"], state);
    }

    #[tokio::test]
    async fn the_deadline_is_still_open_exit_4_naming_the_wait_again() {
        let server = gateway_answering(&[view("opened")]).await;
        let err = wait(&control(&server), ID, fast()).await.unwrap_err();
        assert_eq!(err.exit, EXIT_HANDOFF_ENDED);
        assert_eq!(err.code(), "still_open");
        assert_eq!(
            err.next.as_deref(),
            Some(format!("skardi handoff wait {ID}").as_str())
        );
        assert!(err.message().contains("2026-10-30T12:30:00Z"), "{err:?}");
    }

    #[test]
    fn the_interval_grows_by_half_up_to_ten_seconds() {
        let polling = Polling::HANDOFF;
        let mut interval = polling.first;
        let mut seen = vec![interval];
        for _ in 0..6 {
            interval = polling.grow(interval);
            seen.push(interval);
        }
        let millis: Vec<u128> = seen.iter().map(Duration::as_millis).collect();
        assert_eq!(millis, vec![2000, 3000, 4500, 6750, 10000, 10000, 10000]);
        assert_eq!(polling.deadline, Duration::from_secs(1800));
    }

    #[rstest]
    #[case::seconds("90s", 90)]
    #[case::bare("45", 45)]
    #[case::minutes("30m", 1800)]
    #[case::hours("1h", 3600)]
    fn timeouts_parse(#[case] raw: &str, #[case] seconds: u64) {
        assert_eq!(parse_timeout(raw).unwrap(), Duration::from_secs(seconds));
    }

    #[rstest]
    #[case::zero("0")]
    #[case::negative("-5m")]
    #[case::unit("5d")]
    #[case::words("soon")]
    fn bad_timeouts_are_usage_errors(#[case] raw: &str) {
        assert_eq!(parse_timeout(raw).unwrap_err().exit, EXIT_USAGE);
    }

    /// `--timeout` bounds the command: a gateway that holds one `GET` for
    /// longer than the whole budget is cut off at the deadline, rather than
    /// after the control client's own 130 seconds.
    #[tokio::test]
    async fn a_stalled_gateway_cannot_hold_the_wait_past_its_timeout() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(view("opened"))
                    .set_delay(Duration::from_secs(30)),
            )
            .mount(&server)
            .await;
        let started = std::time::Instant::now();
        let err = wait(&control(&server), ID, fast()).await.unwrap_err();
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "took {:?}",
            started.elapsed()
        );
        assert_eq!(err.exit, EXIT_HANDOFF_ENDED);
        assert_eq!(err.code(), "still_open");
        // Nothing was read, so nothing is claimed about the person.
        assert!(err.message().contains("did not say"), "{err:?}");
        assert_eq!(err.error["handoff"], Value::Null);
        assert_eq!(
            err.next.as_deref(),
            Some(format!("skardi handoff wait {ID}").as_str())
        );
    }

    /// A `Retry-After` is slept inside the request, so it is bounded by the
    /// same deadline: a thirty-second wait does not outlive a short budget.
    #[tokio::test]
    async fn a_rate_limit_wait_longer_than_the_budget_ends_at_the_deadline() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(
                ResponseTemplate::new(429)
                    .insert_header("retry-after", "30")
                    .set_body_json(json!({"error": {"code": "rate_limited", "message": "slow"}})),
            )
            .mount(&server)
            .await;
        // The production `Retry-After` unit: one second per second.
        let control = Control::new(&server.uri(), "acme-prod", "skardi_pat_ctl").unwrap();
        let started = std::time::Instant::now();
        let err = wait(&control, ID, fast()).await.unwrap_err();
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "took {:?}",
            started.elapsed()
        );
        assert_eq!(err.code(), "still_open");
        assert_eq!(server.received_requests().await.unwrap().len(), 1);
    }

    /// A few transport failures in a row are ridden out (a gateway rolling
    /// mid-wait); past that the wait ends as unreachable, exit 5.
    #[tokio::test]
    async fn persistent_transport_failures_end_the_wait_as_unreachable() {
        // Nothing listens on port 1, so every poll is a transport failure.
        // A long deadline: Windows takes about two seconds to refuse a
        // loopback connection, which a short one would cut off first.
        let control = Control::new("http://127.0.0.1:1", "acme-prod", "skardi_pat_ctl").unwrap();
        let polling = fast().with_deadline(Duration::from_secs(60));
        let err = wait(&control, ID, polling).await.unwrap_err();
        assert_eq!(err.exit, EXIT_UNAVAILABLE);
        assert_eq!(err.code(), "control_plane_unreachable");
    }

    #[tokio::test]
    async fn a_completed_handoff_without_an_alias_is_an_invalid_response() {
        let server = gateway_answering(&[view("completed")]).await;
        let err = wait(&control(&server), ID, fast()).await.unwrap_err();
        assert_eq!(err.exit, EXIT_UNAVAILABLE);
        assert_eq!(err.code(), "invalid_response");
        assert_eq!(err.error["handoff"]["state"], "completed");
    }

    #[tokio::test]
    async fn a_state_this_cli_does_not_know_is_named_not_polled_forever() {
        let server = gateway_answering(&[view("archived")]).await;
        let err = wait(&control(&server), ID, fast()).await.unwrap_err();
        assert_eq!(err.exit, EXIT_REFUSED);
        assert_eq!(err.code(), "unexpected_handoff_state");
        assert!(err.message().contains("archived"), "{err:?}");
    }

    #[tokio::test]
    async fn a_refusal_while_waiting_is_reported_at_once() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(404).set_body_json(
                json!({"error": {"code": "handoff_not_found", "message": "no such handoff"}}),
            ))
            .mount(&server)
            .await;
        let err = wait(&control(&server), ID, fast()).await.unwrap_err();
        assert_eq!(err.exit, EXIT_REFUSED);
        assert_eq!(err.code(), "handoff_not_found");
        assert_eq!(server.received_requests().await.unwrap().len(), 1);
    }

    /// `--timeout` takes any number of hours; a deadline past the clock's
    /// range is clamped rather than a panic.
    #[test]
    fn an_unrepresentable_deadline_is_clamped_not_a_panic() {
        let polling = Polling::HANDOFF.with_deadline(Duration::from_secs(u64::MAX));
        assert!(polling.deadline_from_now() > Instant::now() + Duration::from_secs(3600));
    }

    #[tokio::test]
    async fn a_handoff_id_that_is_not_a_uuid_issues_no_request() {
        let server = MockServer::start().await;
        let err = wait(&control(&server), "h-1", fast()).await.unwrap_err();
        assert_eq!(err.exit, EXIT_USAGE);
        assert!(server.received_requests().await.unwrap().is_empty());
    }
}
