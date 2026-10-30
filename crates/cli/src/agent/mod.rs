//! The agent-onboarding commands (skardi-cloud design 2026-10-27 §2.4): the
//! CLI front of the control surface an agent drives to connect a person's
//! data sources, without ever being handed a secret.
//!
//! Ten subcommands, each a thin client of skardi-global's control table as
//! the gateway's `/control/v1` door serves it, plus the data plane's document
//! upload for `documents add`:
//!
//! ```text
//! integrations list | connect | permissions | candidates | install
//!            | cancel | status | sync
//! handoff wait
//! documents add
//! ```
//!
//! # The interface is a contract
//!
//! The console's prompt and the `skardi-onboard` skill are written against
//! what these print, so three things are fixed (§2.4.2):
//!
//! * **Output.** With `--json`, exactly one object on stdout:
//!   `{"ok": true, "result": …, "next": …}` or
//!   `{"ok": false, "error": {"code", "message"}, "next": …}`. `result` is the
//!   route's own response and `error` the server's own envelope, copied
//!   through. Without `--json`, a sentence and the next command.
//! * **`next`.** The onboarding sequence lives HERE, in these hints, and
//!   nowhere else (§2.7): after `connect` it names `handoff wait <id>`, after
//!   `handoff wait` it names `permissions` with the alias and version filled
//!   in, and so on. A prompt or skill that said "follow the next step" stays
//!   right as long as these do.
//! * **Exit codes.** [`EXIT_USAGE`] … [`EXIT_NO_CONTROL`], below.
//!
//! # Two tokens, and which one goes where
//!
//! A `login --control` context holds a pair. The `control` token is sent to
//! `/control/v1` and nowhere else; the `mcp_token` (the context's `token`) to
//! the data plane. **No output carries the control token** (§5.0 invariant
//! 10): the server never echoes it, nothing here formats it, and
//! [`finish`] scrubs it from whatever is printed as a last line of defence.
//!
//! # No state between invocations
//!
//! Separate runs share nothing, so a connection version is always an
//! argument (`--connection-version`) and never remembered: the version the
//! agent names is the one it was handed, which is what the server's version
//! guard compares against (§2.2.5).

pub mod control;
pub mod documents;
pub mod handoff;
pub mod integrations;
pub mod scope;

use crate::config::{self, ClientConfig, ContextMode};
use crate::exit::Exit;
use chrono::{DateTime, Utc};
use serde_json::{Value, json};
use std::fmt;
use std::path::Path;

/// Done.
pub const EXIT_OK: u8 = 0;
/// A usage error: a bad flag, a malformed `--scope`, a file that cannot be
/// read.
pub const EXIT_USAGE: u8 = 2;
/// The server refused: any 4xx not covered below.
pub const EXIT_REFUSED: u8 = 3;
/// A handoff ended without a result — `expired`, `denied`, `abandoned` — or
/// `handoff wait` reached its deadline with the handoff `still_open`.
pub const EXIT_HANDOFF_ENDED: u8 = 4;
/// The control plane could not be reached, or answered 5xx.
pub const EXIT_UNAVAILABLE: u8 = 5;
/// No usable `control` token: none was granted, or it has expired.
pub const EXIT_NO_CONTROL: u8 = 6;

/// The command that mints a control token, named by every exit-6 hint.
pub const LOGIN_CONTROL: &str = "skardi login --control";

/// A failed agent command: the exit code, the error object the envelope
/// carries, and the command that recovers, when there is one.
///
/// `error` is a JSON object with at least `code` and `message`. For a server
/// refusal it is the server's own envelope, copied through with every field it
/// carried; for a local failure it is built here.
#[derive(Debug, Clone)]
pub struct AgentError {
    pub exit: u8,
    pub error: Value,
    pub next: Option<String>,
}

impl AgentError {
    /// A failure decided locally, before or without a server's answer.
    pub fn local(exit: u8, code: &str, message: impl Into<String>) -> AgentError {
        AgentError {
            exit,
            error: json!({ "code": code, "message": message.into() }),
            next: None,
        }
    }

    /// A usage error (exit 2).
    pub fn usage(code: &str, message: impl Into<String>) -> AgentError {
        AgentError::local(EXIT_USAGE, code, message)
    }

    pub fn with_next(mut self, next: impl Into<String>) -> AgentError {
        self.next = Some(next.into());
        self
    }

    /// Attach `value` under `key` in the error object, beside the server's
    /// own fields.
    pub fn with_field(mut self, key: &str, value: Value) -> AgentError {
        if let Some(object) = self.error.as_object_mut() {
            object.insert(key.to_string(), value);
        }
        self
    }

    pub fn code(&self) -> &str {
        self.error
            .get("code")
            .and_then(Value::as_str)
            .unwrap_or("unknown")
    }

    pub fn message(&self) -> &str {
        self.error
            .get("message")
            .and_then(Value::as_str)
            .unwrap_or_default()
    }
}

impl fmt::Display for AgentError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} ({})", self.message(), self.code())
    }
}

/// A command that succeeded.
#[derive(Debug, Clone)]
pub struct Outcome {
    /// The route's own response (for `install`, the final operation).
    pub result: Value,
    /// What the plain rendering says first.
    pub sentence: String,
    /// Whether the plain rendering prints `result` too. Off for commands
    /// whose sentence already says everything worth reading.
    pub show_result: bool,
    pub next: Option<String>,
    /// Advisory lines for stderr, in either mode. stdout stays one object
    /// under `--json`, so anything a person should see beside it goes here.
    pub notes: Vec<String>,
}

impl Outcome {
    pub fn new(result: Value, sentence: impl Into<String>) -> Outcome {
        Outcome {
            result,
            sentence: sentence.into(),
            show_result: false,
            next: None,
            notes: Vec::new(),
        }
    }

    pub fn showing_result(mut self) -> Outcome {
        self.show_result = true;
        self
    }

    pub fn with_next(mut self, next: impl Into<String>) -> Outcome {
        self.next = Some(next.into());
        self
    }

    pub fn with_note(mut self, note: impl Into<String>) -> Outcome {
        self.notes.push(note.into());
        self
    }
}

/// How the run was invoked, for rendering and for the `next` hints, which
/// repeat the flags that change WHERE the next command runs.
#[derive(Debug, Clone, Default)]
pub struct Invocation {
    pub json: bool,
    /// `--context NAME`, when it was given: a next command without it would
    /// run against the current context instead.
    pub context: Option<String>,
}

impl Invocation {
    /// `next` as it should be printed: the bare command, plus `--json` when
    /// this run used it, so an agent can run the hint verbatim and get the
    /// shape it parsed, plus `--context` when this run named one.
    pub fn decorate(&self, next: &str) -> String {
        // A hint that is not a skardi command (`skardi login --control` is,
        // so it qualifies) is left alone; every hint here is one today.
        let mut out = next.to_string();
        if let Some(context) = &self.context
            && next.starts_with("skardi ")
        {
            out.push_str(" --context ");
            out.push_str(&shell_quote(context));
        }
        if self.json && next.starts_with("skardi ") && !next.starts_with("skardi login") {
            out.push_str(" --json");
        }
        out
    }
}

/// What one run prints: stdout and stderr, already rendered.
///
/// Built by a pure function so tests assert the exact text — including that
/// no rendering of any command carries the control token.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Rendered {
    pub stdout: String,
    pub stderr: String,
    pub exit: u8,
}

/// Render an outcome for `invocation`, scrubbing `secret` from every line.
pub fn render(
    outcome: &Result<Outcome, AgentError>,
    invocation: &Invocation,
    secret: Option<&str>,
) -> Rendered {
    let next = |hint: &Option<String>| hint.as_deref().map(|n| invocation.decorate(n));
    let (stdout, stderr, exit) = match outcome {
        Ok(outcome) => {
            let next = next(&outcome.next);
            let notes: String = outcome
                .notes
                .iter()
                .map(|note| format!("note: {note}\n"))
                .collect();
            let stdout = if invocation.json {
                let envelope = json!({ "ok": true, "result": outcome.result, "next": next });
                format!("{}\n", compact(&envelope))
            } else {
                let mut out = format!("{}\n", outcome.sentence);
                if outcome.show_result {
                    out.push_str(&pretty(&outcome.result));
                    out.push('\n');
                }
                if let Some(next) = &next {
                    out.push_str(&format!("next: {next}\n"));
                }
                out
            };
            (stdout, notes, EXIT_OK)
        }
        Err(err) => {
            let next = next(&err.next);
            if invocation.json {
                let envelope = json!({ "ok": false, "error": err.error, "next": next });
                (format!("{}\n", compact(&envelope)), String::new(), err.exit)
            } else {
                let mut out = format!("error: {err}\n");
                if let Some(next) = &next {
                    out.push_str(&format!("next: {next}\n"));
                }
                (String::new(), out, err.exit)
            }
        }
    };
    Rendered {
        stdout: scrub(stdout, secret),
        stderr: scrub(stderr, secret),
        exit,
    }
}

/// Print a rendered run and turn its exit code into the command's result.
pub fn finish(
    outcome: Result<Outcome, AgentError>,
    invocation: &Invocation,
    secret: Option<&str>,
) -> anyhow::Result<()> {
    let rendered = render(&outcome, invocation, secret);
    print!("{}", rendered.stdout);
    eprint!("{}", rendered.stderr);
    if rendered.exit == EXIT_OK {
        Ok(())
    } else {
        Err(Exit::reported(rendered.exit).into())
    }
}

/// What a scrubbed secret reads as, the same word `config view` uses.
const REDACTED: &str = "(redacted)";

/// Replace every occurrence of `secret` with `(redacted)`.
///
/// Nothing in these commands formats the control token, and the server never
/// returns one; this exists because invariant 10 is about what reaches the
/// agent, not about what this code intends, and a relayed server message is
/// text this binary did not write.
fn scrub(text: String, secret: Option<&str>) -> String {
    match secret {
        Some(secret) if !secret.is_empty() && text.contains(secret) => {
            text.replace(secret, REDACTED)
        }
        _ => text,
    }
}

fn compact(value: &Value) -> String {
    serde_json::to_string(value).expect("a serde_json::Value always serializes")
}

fn pretty(value: &Value) -> String {
    serde_json::to_string_pretty(value).expect("a serde_json::Value always serializes")
}

/// Quote `value` for a POSIX shell when it needs it, so a `next` hint carrying
/// a cursor, a coordinate or a scope can be pasted as-is.
pub fn shell_quote(value: &str) -> String {
    let plain = !value.is_empty()
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"._-/:@=+,".contains(&b));
    if plain {
        value.to_string()
    } else {
        format!("'{}'", value.replace('\'', r"'\''"))
    }
}

/// The context an agent command runs against: where the gateway is, which
/// workspace, and the two tokens.
///
/// `Debug` is hand-written: this carries both credentials.
pub struct AgentContext {
    pub name: String,
    /// The gateway: the context's `server`, or `--server`.
    pub gateway: String,
    /// The workspace's slug, as `login` wrote it.
    pub workspace: String,
    pub control_token: Option<String>,
    pub control_expires_at: Option<String>,
    /// The data-plane token, for `documents add`'s upload.
    pub mcp_token: Option<String>,
}

impl fmt::Debug for AgentContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AgentContext")
            .field("name", &self.name)
            .field("gateway", &self.gateway)
            .field("workspace", &self.workspace)
            .field(
                "control_token",
                &self.control_token.as_ref().map(|_| "(redacted)"),
            )
            .field("control_expires_at", &self.control_expires_at)
            .field("mcp_token", &self.mcp_token.as_ref().map(|_| "(redacted)"))
            .finish()
    }
}

impl AgentContext {
    /// Resolve the context the same way every other command does — so a stray
    /// `$SKARDI_SERVER_URL` is refused here exactly as it is for `query` —
    /// and then read the pair's fields off the selected context.
    ///
    /// `--token` is refused: the control token comes from `login --control`
    /// and nowhere else, and a flag would put it in shell history and in the
    /// process list.
    pub fn resolve(
        flag_server: Option<String>,
        flag_token: Option<String>,
        flag_context: Option<String>,
        path: Option<&Path>,
    ) -> Result<AgentContext, AgentError> {
        if flag_token.is_some() {
            return Err(AgentError::usage(
                "token_flag_not_accepted",
                "--token is not accepted here: these commands use the control token `skardi login --control` stored, and the context's own query token",
            ));
        }
        let resolved = ClientConfig::resolve(flag_server, None, flag_context)
            .map_err(|err| AgentError::usage("config_error", err.to_string()))?;
        let file = path.and_then(config::load);
        AgentContext::from_resolved(&resolved, file.as_ref())
    }

    /// The pure half of [`Self::resolve`], for tests.
    pub fn from_resolved(
        resolved: &ClientConfig,
        file: Option<&config::ContextsFile>,
    ) -> Result<AgentContext, AgentError> {
        let Some(selected) = resolved.cloud_context() else {
            return Err(AgentError::local(
                EXIT_NO_CONTROL,
                "not_a_cloud_context",
                "these commands connect a skardi-cloud workspace's data sources, and the selected context is not a skardi-cloud one",
            )
            .with_next(LOGIN_CONTROL));
        };
        let context = file
            .map(|f| f.effective_contexts())
            .unwrap_or_default()
            .into_iter()
            .find(|c| c.name == selected.name && c.mode == ContextMode::Cloud);
        let non_empty = |value: Option<String>| {
            value
                .map(|v| v.trim().to_string())
                .filter(|v| !v.is_empty())
        };
        Ok(AgentContext {
            name: selected.name.clone(),
            gateway: resolved.server.clone(),
            workspace: selected.workspace.clone().unwrap_or_default(),
            control_token: non_empty(context.as_ref().and_then(|c| c.control_token.clone())),
            control_expires_at: non_empty(
                context
                    .as_ref()
                    .and_then(|c| c.control_token_expires_at.clone()),
            ),
            mcp_token: resolved.token.clone(),
        })
    }

    /// The control token, or exit 6 before any request is sent.
    ///
    /// Expiry is checked locally from the stamp `login` wrote, so a token
    /// past its 24 hours costs no round trip; an unparsable stamp is left to
    /// the server, which answers `token_unknown_or_revoked` and maps to the
    /// same exit.
    pub fn control(&self, now: DateTime<Utc>) -> Result<&str, AgentError> {
        let Some(token) = self.control_token.as_deref() else {
            return Err(AgentError::local(
                EXIT_NO_CONTROL,
                "control_not_granted",
                format!(
                    "context '{}' holds no control token: it was signed in without --control, or the approval came back without it. Run `{LOGIN_CONTROL}` and tick \"may connect data sources\"",
                    self.name
                ),
            )
            .with_next(LOGIN_CONTROL));
        };
        if let Some(raw) = self.control_expires_at.as_deref()
            && let Ok(at) = DateTime::parse_from_rfc3339(raw)
            && at.with_timezone(&Utc) <= now
        {
            return Err(AgentError::local(
                EXIT_NO_CONTROL,
                "control_token_expired",
                format!(
                    "the control token for context '{}' expired at {raw} (it lives 24 hours). Queries still work; run `{LOGIN_CONTROL}` to connect more",
                    self.name
                ),
            )
            .with_next(LOGIN_CONTROL));
        }
        Ok(token)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{Context, ContextsFile, SelectedContext};

    fn resolved(mode: ContextMode) -> ClientConfig {
        ClientConfig {
            server: "https://gw.example".to_string(),
            token: Some("skardi_pat_mcp".to_string()),
            context: Some(SelectedContext {
                name: "acme/prod".to_string(),
                mode,
                workspace: Some("acme-prod".to_string()),
                token_expires_at: None,
            }),
        }
    }

    fn file(control: Option<&str>, expires: Option<&str>) -> ContextsFile {
        ContextsFile {
            contexts: vec![Context {
                name: "acme/prod".to_string(),
                mode: ContextMode::Cloud,
                server: Some("https://gw.example".to_string()),
                workspace: Some("acme-prod".to_string()),
                token: Some("skardi_pat_mcp".to_string()),
                control_token: control.map(str::to_string),
                control_token_expires_at: expires.map(str::to_string),
                ..Context::default()
            }],
            ..ContextsFile::default()
        }
    }

    fn now() -> DateTime<Utc> {
        DateTime::parse_from_rfc3339("2026-10-30T12:00:00Z")
            .unwrap()
            .with_timezone(&Utc)
    }

    #[test]
    fn a_context_with_a_live_control_token_hands_it_out() {
        let ctx = AgentContext::from_resolved(
            &resolved(ContextMode::Cloud),
            Some(&file(Some("skardi_pat_ctl"), Some("2026-10-31T12:00:00Z"))),
        )
        .unwrap();
        assert_eq!(ctx.control(now()).unwrap(), "skardi_pat_ctl");
        assert_eq!(ctx.workspace, "acme-prod");
        // Debug never shows either token.
        let rendered = format!("{ctx:?}");
        assert!(!rendered.contains("skardi_pat_ctl"), "{rendered}");
        assert!(!rendered.contains("skardi_pat_mcp"), "{rendered}");
    }

    #[rstest::rstest]
    #[case::none_granted(None, None, "control_not_granted")]
    #[case::expired(
        Some("skardi_pat_ctl"),
        Some("2026-10-30T11:59:59Z"),
        "control_token_expired"
    )]
    fn no_usable_control_token_is_exit_6_naming_login_control(
        #[case] token: Option<&str>,
        #[case] expires: Option<&str>,
        #[case] code: &str,
    ) {
        let ctx =
            AgentContext::from_resolved(&resolved(ContextMode::Cloud), Some(&file(token, expires)))
                .unwrap();
        let err = ctx.control(now()).unwrap_err();
        assert_eq!(err.exit, EXIT_NO_CONTROL);
        assert_eq!(err.code(), code);
        assert_eq!(err.next.as_deref(), Some(LOGIN_CONTROL));
    }

    #[test]
    fn a_server_context_is_exit_6_rather_than_a_request_to_the_wrong_place() {
        let err = AgentContext::from_resolved(&resolved(ContextMode::Server), None).unwrap_err();
        assert_eq!(err.exit, EXIT_NO_CONTROL);
        assert_eq!(err.code(), "not_a_cloud_context");
    }

    #[test]
    fn the_token_flag_is_a_usage_error() {
        let err = AgentContext::resolve(None, Some("x".to_string()), None, None).unwrap_err();
        assert_eq!(err.exit, EXIT_USAGE);
    }

    /// §2.4.2's two shapes, exactly, on one line each.
    #[test]
    fn json_output_is_one_object_in_either_shape() {
        let invocation = Invocation {
            json: true,
            context: None,
        };
        let ok = render(
            &Ok(Outcome::new(json!({"a": 1}), "done").with_next("skardi handoff wait h1")),
            &invocation,
            None,
        );
        let parsed: Value = serde_json::from_str(ok.stdout.trim()).unwrap();
        assert_eq!(
            parsed,
            json!({"ok": true, "result": {"a": 1}, "next": "skardi handoff wait h1 --json"})
        );
        assert_eq!(ok.exit, EXIT_OK);
        assert_eq!(ok.stdout.lines().count(), 1);

        let err = render(
            &Err(AgentError::local(EXIT_REFUSED, "admin_required", "nope")),
            &invocation,
            None,
        );
        let parsed: Value = serde_json::from_str(err.stdout.trim()).unwrap();
        assert_eq!(
            parsed,
            json!({"ok": false, "error": {"code": "admin_required", "message": "nope"}, "next": null})
        );
        assert_eq!(err.exit, EXIT_REFUSED);
        assert!(err.stderr.is_empty(), "stdout is the only channel: {err:?}");
    }

    #[test]
    fn plain_output_is_a_sentence_and_the_next_command() {
        let invocation = Invocation {
            json: false,
            context: Some("acme/prod".to_string()),
        };
        let ok = render(
            &Ok(Outcome::new(json!({}), "open this link").with_next("skardi handoff wait h1")),
            &invocation,
            None,
        );
        assert_eq!(
            ok.stdout,
            "open this link\nnext: skardi handoff wait h1 --context acme/prod\n"
        );
        let err = render(
            &Err(
                AgentError::local(EXIT_NO_CONTROL, "control_not_granted", "no token")
                    .with_next(LOGIN_CONTROL),
            ),
            &invocation,
            None,
        );
        assert_eq!(
            err.stderr,
            "error: no token (control_not_granted)\nnext: skardi login --control --context acme/prod\n"
        );
        assert!(err.stdout.is_empty());
    }

    #[test]
    fn a_secret_is_scrubbed_from_every_channel() {
        let invocation = Invocation::default();
        let rendered = render(
            &Err(AgentError::local(
                EXIT_REFUSED,
                "x",
                "echoed skardi_pat_ctl back",
            )),
            &invocation,
            Some("skardi_pat_ctl"),
        );
        assert!(!rendered.stderr.contains("skardi_pat_ctl"), "{rendered:?}");
        assert!(rendered.stderr.contains("(redacted)"), "{rendered:?}");
    }

    #[rstest::rstest]
    #[case::plain("acme-notion", "acme-notion")]
    #[case::space("a b", "'a b'")]
    #[case::json(r#"{"k":"v"}"#, r#"'{"k":"v"}'"#)]
    #[case::quote("it's", r"'it'\''s'")]
    #[case::empty("", "''")]
    fn shell_quote_leaves_plain_words_and_quotes_the_rest(#[case] raw: &str, #[case] quoted: &str) {
        assert_eq!(shell_quote(raw), quoted);
    }
}
