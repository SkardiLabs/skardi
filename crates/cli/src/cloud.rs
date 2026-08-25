//! What a `mode: cloud` context changes about a command's outcome (§8).
//!
//! Three things, all of them pre- or post-processing around an unchanged
//! request path — `mode` never selects a URL, so nothing here touches routing:
//!
//! 1. **Capability gating.** A skardi-cloud gateway mounts `query` and
//!    `schema`; it has no jobs, pipelines, or health surface. Those commands
//!    fail before a request is built, because a 404 from a gateway that never
//!    served the route reads as "the server is broken" rather than "this
//!    context cannot do that".
//! 2. **Credential expiry.** A context carries `token-expires-at`, so an
//!    expired PAT is knowable without spending a round trip.
//! 3. **Error translation.** The gateway's typed refusals name the *deployment*
//!    ("the token's org has no workspace matching the one this gateway
//!    serves"); the operator thinks in contexts. Translation happens here, at
//!    the one place that knows both.
//!
//! Everything is a pure function of `(error, capability, config)` so the
//! matrix is unit-testable without a server.

use crate::client::ApiError;
use crate::config::ClientConfig;
use anyhow::anyhow;
use chrono::{DateTime, Utc};

/// One remote subcommand, for gating and for the error mapping that is
/// specific to one route (§7.4.2's schema-read limit).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Capability {
    Query,
    Schema,
    Run,
    Pipeline,
    Job,
    Health,
}

impl Capability {
    /// The subcommand name as the user typed it.
    ///
    /// §8's example message says `'jobs'`; this CLI's subcommand is `job`
    /// (`skardi job list`), and naming a command that does not exist would be
    /// a worse message than the design's illustration.
    pub const fn command(self) -> &'static str {
        match self {
            Capability::Query => "query",
            Capability::Schema => "schema",
            Capability::Run => "run",
            Capability::Pipeline => "pipeline",
            Capability::Job => "job",
            Capability::Health => "health",
        }
    }

    /// Whether a skardi-cloud gateway serves this command.
    ///
    /// The gateway's route table is `POST /query` and `GET /data_source`
    /// (§7.4); everything else is an engine-local surface that only a
    /// `mode: server` context reaches.
    pub const fn served_by_gateway(self) -> bool {
        matches!(self, Capability::Query | Capability::Schema)
    }
}

/// Every capability, so the "Available:" list is DERIVED from
/// [`Capability::served_by_gateway`] rather than restated next to it — a
/// hand-written list is the kind that survives the table changing under it.
const ALL_CAPABILITIES: [Capability; 6] = [
    Capability::Query,
    Capability::Schema,
    Capability::Run,
    Capability::Pipeline,
    Capability::Job,
    Capability::Health,
];

/// The comma-separated commands a cloud context can run.
fn cloud_capability_list() -> String {
    ALL_CAPABILITIES
        .iter()
        .filter(|c| c.served_by_gateway())
        .map(|c| c.command())
        .collect::<Vec<_>>()
        .join(", ")
}

/// Refuse a command the selected cloud context cannot serve, before any
/// request is built (§8, row 1). Exits `1` through `main`'s mapping — `2` is
/// reserved for "cannot reach the server", and this never reached it.
pub fn ensure_available(capability: Capability, config: &ClientConfig) -> anyhow::Result<()> {
    let Some(context) = config.cloud_context() else {
        return Ok(());
    };
    if capability.served_by_gateway() {
        return Ok(());
    }
    Err(anyhow!(
        "'{}' is not available in a cloud context ({}). Available: {}.",
        capability.command(),
        context.name,
        cloud_capability_list()
    ))
}

/// Refuse when the stored PAT's `token-expires-at` has already passed (§8,
/// row 3), so an expired credential costs no round trip.
///
/// `now` is a parameter rather than a `Utc::now()` call so the check stays a
/// pure function, the way resolution is.
pub fn ensure_credential_fresh(config: &ClientConfig, now: DateTime<Utc>) -> anyhow::Result<()> {
    let Some(context) = config.cloud_context() else {
        return Ok(());
    };
    let Some(raw) = context.token_expires_at.as_deref() else {
        return Ok(());
    };
    // An unparsable stamp is NOT fatal. It is an optional field in a file a
    // human may have hand-edited, and refusing every command over a malformed
    // annotation would be a worse outcome than sending one request the gateway
    // answers authoritatively.
    let Ok(expires_at) = DateTime::parse_from_rfc3339(raw) else {
        return Ok(());
    };
    if expires_at.with_timezone(&Utc) > now {
        return Ok(());
    }
    // §8 prescribes the 401 message verbatim here ("was rejected"). Said
    // pre-flight that is untrue — nothing rejected anything, no request was
    // sent — so this keeps the design's shape (context named, `skardi login`
    // as the action) and states what was actually observed.
    Err(anyhow!(
        "credential for context '{}' expired at {raw}. Run 'skardi login'.",
        context.name
    ))
}

/// Rewrite a gateway failure into the context's vocabulary (§8, rows 2, 4, 5).
///
/// Non-cloud contexts and unrecognised failures pass through untouched: this
/// only ever replaces a message it can improve. `ApiError::Connect` is never
/// touched either — its own message already names the URL and the flags, and
/// `main` reads the same error back out to choose exit code `2`.
pub fn diagnose(
    err: anyhow::Error,
    capability: Capability,
    config: &ClientConfig,
) -> anyhow::Error {
    let Some(context) = config.cloud_context() else {
        return err;
    };
    let Some(ApiError::Http {
        status,
        error_type,
        message,
        retry_after,
    }) = err.downcast_ref::<ApiError>()
    else {
        return err;
    };

    match *status {
        // REPLACED, not annotated: the transport-level 401 message tells the
        // caller to set SKARDI_API_TOKEN or edit `token` in the config, and
        // for a cloud context that advice is actively wrong — resolution
        // refuses the env var (§5.1) and the PAT is `login`'s to write.
        401 => anyhow!(
            "credential for context '{}' was rejected — it may be expired or revoked. Run 'skardi login'.",
            context.name
        ),
        // ANNOTATED, not replaced: the gateway's own sentence says which
        // boundary was crossed (not a member / no matching workspace), which
        // is the detail that distinguishes the two, and it never leaks a slug
        // the caller didn't already send.
        403 if names_workspace(error_type.as_deref(), message) => err.context(format!(
            "context '{}' names workspace '{}', which this gateway does not serve",
            context.name,
            context.workspace.as_deref().unwrap_or("(unset)"),
        )),
        // §7.4.2's schema-read limiter is a LOAD signal, not a schema error,
        // and `Retry-After` is what separates it from a gateway that is simply
        // down. Scoped to `schema` because that is the only route that
        // bounds concurrency this way.
        503 if capability == Capability::Schema
            && let Some(seconds) = *retry_after =>
        {
            anyhow!("the gateway is at its schema-read limit; retry in {seconds}s")
        }
        _ => err,
    }
}

/// Whether a `403` is the workspace boundary rather than an authorization
/// failure inside the workspace.
///
/// Today's gateway answers every `PatAuthError::NotAuthorized` with
/// `error_type: "forbidden"` and a deliberately caller-safe message that names
/// no slug ("the token's org has no workspace matching the one this gateway
/// serves"), so the word in the message is the only discriminator available.
/// The typed tokens are matched too: §7.3's org-admission path maps
/// `Directory.Resolve`'s `workspace_not_found` to a typed 403, and accepting
/// them now means the CLI reads that release without needing one of its own.
///
/// `identity_required` and `identity_unresolved` also mention "workspace" —
/// they are excluded because they carry their own `error_type`, and they are
/// genuinely a different failure (a credential with no envelope, not a slug
/// the deployment refuses).
fn names_workspace(error_type: Option<&str>, message: &str) -> bool {
    match error_type {
        Some("workspace_not_found" | "workspace_required") => true,
        Some("forbidden") => message.to_ascii_lowercase().contains("workspace"),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ALL_CAPABILITIES, Capability, diagnose, ensure_available, ensure_credential_fresh,
    };
    use crate::client::ApiError;
    use crate::config::{ClientConfig, ContextMode, SelectedContext};
    use chrono::{DateTime, Utc};

    fn at(rfc3339: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(rfc3339)
            .unwrap()
            .with_timezone(&Utc)
    }

    fn config(mode: Option<ContextMode>, expires: Option<&str>) -> ClientConfig {
        ClientConfig {
            server: "https://gw.example".to_string(),
            token: Some("pat".to_string()),
            context: mode.map(|mode| SelectedContext {
                name: "acme/prod".to_string(),
                mode,
                workspace: Some("acme-prod".to_string()),
                token_expires_at: expires.map(str::to_string),
            }),
        }
    }

    fn http(
        status: u16,
        error_type: Option<&str>,
        message: &str,
        retry_after: Option<u64>,
    ) -> anyhow::Error {
        anyhow::Error::new(ApiError::Http {
            status,
            error_type: error_type.map(str::to_string),
            message: message.to_string(),
            retry_after,
        })
    }

    /// §10's "table test over (subcommand, mode)". The mode column is what
    /// decides, so both non-cloud shapes (server context, no context at all)
    /// are exercised for every capability.
    #[test]
    fn gating_matrix_refuses_exactly_the_engine_local_commands() {
        for capability in ALL_CAPABILITIES {
            let cloud = ensure_available(capability, &config(Some(ContextMode::Cloud), None));
            assert_eq!(
                cloud.is_err(),
                !capability.served_by_gateway(),
                "cloud gating disagreed with served_by_gateway for '{}'",
                capability.command()
            );
            for non_cloud in [Some(ContextMode::Server), None] {
                assert!(
                    ensure_available(capability, &config(non_cloud, None)).is_ok(),
                    "'{}' must stay available without a cloud context",
                    capability.command()
                );
            }
        }
    }

    #[test]
    fn refusal_names_the_command_the_context_and_what_is_left() {
        let err = ensure_available(Capability::Job, &config(Some(ContextMode::Cloud), None))
            .unwrap_err()
            .to_string();
        assert_eq!(
            err,
            "'job' is not available in a cloud context (acme/prod). Available: query, schema."
        );
    }

    #[test]
    fn expiry_is_checked_only_for_a_cloud_context_and_only_when_it_has_passed() {
        let now = at("2026-06-01T12:00:00Z");

        let expired = ensure_credential_fresh(
            &config(Some(ContextMode::Cloud), Some("2026-05-31T12:00:00Z")),
            now,
        );
        assert_eq!(
            expired.unwrap_err().to_string(),
            "credential for context 'acme/prod' expired at 2026-05-31T12:00:00Z. Run 'skardi login'."
        );

        // Fresh, absent, unparsable, and non-cloud all pass without a request.
        for (mode, expires) in [
            (Some(ContextMode::Cloud), Some("2026-06-01T12:00:01Z")),
            (Some(ContextMode::Cloud), None),
            (Some(ContextMode::Cloud), Some("not-a-timestamp")),
            (Some(ContextMode::Server), Some("2020-01-01T00:00:00Z")),
            (None, None),
        ] {
            assert!(
                ensure_credential_fresh(&config(mode, expires), now).is_ok(),
                "unexpected refusal for mode={mode:?} expires={expires:?}"
            );
        }
    }

    /// A non-UTC offset is a legal RFC 3339 stamp and must be compared as an
    /// instant, not as text: this one is 23:00Z, an hour AFTER `now`, while
    /// its digits sort before it.
    #[test]
    fn expiry_compares_instants_not_strings() {
        assert!(
            ensure_credential_fresh(
                &config(Some(ContextMode::Cloud), Some("2026-06-01T18:00:00-05:00")),
                at("2026-06-01T22:00:00Z"),
            )
            .is_ok()
        );
    }

    #[test]
    fn unauthorized_is_replaced_with_the_context_and_the_login_action() {
        let err = diagnose(
            http(401, Some("unauthorized"), "invalid token", None),
            Capability::Query,
            &config(Some(ContextMode::Cloud), None),
        );
        assert_eq!(
            format!("{err:#}"),
            "credential for context 'acme/prod' was rejected — it may be expired or revoked. Run 'skardi login'."
        );
        // Replaced outright: the transport message's advice (set
        // SKARDI_API_TOKEN) is refused by resolution for a cloud context.
        assert!(!format!("{err:#}").contains("SKARDI_API_TOKEN"));
    }

    #[test]
    fn workspace_boundary_403_names_the_context_and_keeps_the_gateway_reason() {
        let err = diagnose(
            http(
                403,
                Some("forbidden"),
                "the token's org has no workspace matching the one this gateway serves",
                None,
            ),
            Capability::Query,
            &config(Some(ContextMode::Cloud), None),
        );
        let rendered = format!("{err:#}");
        assert!(
            rendered.starts_with(
                "context 'acme/prod' names workspace 'acme-prod', which this gateway does not serve"
            ),
            "rendered: {rendered}"
        );
        assert!(
            rendered.contains("no workspace matching"),
            "rendered: {rendered}"
        );
        // Still an ApiError underneath, so `main` keeps choosing exit 1 (not
        // 2) from the same downcast.
        assert!(matches!(
            err.downcast_ref::<ApiError>(),
            Some(ApiError::Http { status: 403, .. })
        ));
    }

    /// The 403s that are NOT the workspace boundary pass through untouched,
    /// including `identity_required` — whose message mentions "workspace" and
    /// would be misclassified by a message-only rule.
    #[test]
    fn other_403s_are_left_alone() {
        for (error_type, message) in [
            (
                Some("identity_required"),
                "this deployment routes queries per workspace; the presented credential resolved locally",
            ),
            (
                Some("identity_unresolved"),
                "resolved identity could not be turned into a request envelope",
            ),
            (
                Some("forbidden"),
                "the caller's role does not permit this operation",
            ),
            (None, "forbidden"),
        ] {
            let err = diagnose(
                http(403, error_type, message, None),
                Capability::Query,
                &config(Some(ContextMode::Cloud), None),
            );
            assert!(
                !format!("{err:#}").contains("does not serve"),
                "{error_type:?} was rewritten as a workspace boundary miss"
            );
        }
    }

    #[test]
    fn schema_read_limit_is_reported_as_load_only_with_retry_after() {
        let cloud = config(Some(ContextMode::Cloud), None);
        let limited = diagnose(
            http(
                503,
                Some("schema_read_limit"),
                "too many concurrent schema reads",
                Some(7),
            ),
            Capability::Schema,
            &cloud,
        );
        assert_eq!(
            format!("{limited:#}"),
            "the gateway is at its schema-read limit; retry in 7s"
        );

        // No Retry-After: a plain outage, reported as the gateway phrased it.
        let outage = diagnose(
            http(503, Some("backend_unavailable"), "engine unreachable", None),
            Capability::Schema,
            &cloud,
        );
        assert!(format!("{outage:#}").contains("engine unreachable"));

        // Same 503 on another route is not the schema limiter.
        let query = diagnose(
            http(
                503,
                Some("backend_unavailable"),
                "engine unreachable",
                Some(7),
            ),
            Capability::Query,
            &cloud,
        );
        assert!(format!("{query:#}").contains("engine unreachable"));
    }

    #[test]
    fn non_cloud_contexts_and_transport_failures_pass_through() {
        for mode in [Some(ContextMode::Server), None] {
            let err = diagnose(
                http(401, Some("unauthorized"), "invalid token", None),
                Capability::Query,
                &config(mode, None),
            );
            assert!(!format!("{err:#}").contains("skardi login"));
        }

        let connect = diagnose(
            anyhow::Error::new(ApiError::Connect {
                url: "https://gw.example/query".to_string(),
                message: "connection refused".to_string(),
            }),
            Capability::Query,
            &config(Some(ContextMode::Cloud), None),
        );
        assert!(matches!(
            connect.downcast_ref::<ApiError>(),
            Some(ApiError::Connect { .. })
        ));
    }
}
