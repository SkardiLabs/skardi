//! Tracing filter construction, including the hard floor that keeps
//! value-bearing query plans out of the log/OTLP stream.
//!
//! `POST /query` deliberately never logs the raw statement (see
//! [`crate::query_handlers`]) because callers may inline literal secrets or
//! PII. Suppressing the handler's own `sql` field is not enough on its own:
//! DataFusion reconstructs the *same literals* inside the plans it prints at
//! DEBUG, e.g.
//!
//! ```text
//! Projection: Utf8("TOP_SECRET")            <- datafusion_optimizer::utils::log_plan
//! ProjectionExec: expr=[TOP_SECRET as ...]  <- datafusion-tracing span field
//! ```
//!
//! Those lines flow through the same subscriber — and therefore the same OTLP
//! exporter — as everything else. So the confidentiality guarantee is enforced
//! here, at the filter: every target known to print plans is pinned to INFO and
//! cannot be lowered by `RUST_LOG`.
//!
//! An operator who is knowingly debugging a planning problem on non-sensitive
//! data can lift the floor with `SKARDI_ALLOW_PLAN_VALUE_LOGGING=1`. It is an
//! explicit, separate opt-in precisely because it re-enables value export.

use tracing_subscriber::EnvFilter;

/// Env var that lifts the plan-value logging floor. Any value other than
/// `0`/`false`/`no`/empty enables plan logging.
pub const ALLOW_PLAN_VALUE_LOGGING_ENV: &str = "SKARDI_ALLOW_PLAN_VALUE_LOGGING";

/// Tracing target used for the datafusion-tracing execution/rule spans this
/// server installs (see [`crate::server::setup_app_state`]).
///
/// Those macros default their target to `module_path!()` of the *call site*,
/// which would bury the spans under `skardi_server::server` where no filter
/// could single them out. Naming the target explicitly is what makes the floor
/// below able to cover them.
pub const QUERY_PLAN_TARGET: &str = "skardi_query_plan";

/// Target prefixes whose DEBUG/TRACE records embed query plans, and with them
/// the literal values from the statement.
///
/// Matching is by prefix, mirroring `EnvFilter`'s own target matching, so
/// `datafusion` also covers `datafusion_optimizer`, `datafusion_sql`,
/// `datafusion_physical_optimizer`, `datafusion_federation` and
/// `datafusion_tracing`.
const PLAN_VALUE_TARGET_PREFIXES: &[&str] = &["datafusion", QUERY_PLAN_TARGET, "sqlparser"];

/// Build the tracing filter from the given `RUST_LOG` value (`None` when the
/// variable is unset or invalid unicode), defaulting to `info`.
///
/// Two hard caps are applied on top of whatever the operator asked for:
///
/// * aws_config logs the AWS access key id in plaintext at INFO when resolving
///   credentials, so it is capped at WARN unless `RUST_LOG` explicitly opts in.
/// * Every target in [`PLAN_VALUE_TARGET_PREFIXES`] is pinned to INFO unless
///   `allow_plan_value_logging` is set. Directives in `RUST_LOG` that would
///   lower one of those targets are dropped before parsing, so neither a global
///   `RUST_LOG=debug` nor a targeted `RUST_LOG=datafusion_optimizer=debug` can
///   put query literals on the wire.
pub fn build_env_filter(rust_log: Option<&str>, allow_plan_value_logging: bool) -> EnvFilter {
    let retained: Vec<&str> = match rust_log {
        Some(v) if !allow_plan_value_logging => v
            .split(',')
            .filter(|d| !directive_leaks_plan_values(d))
            .collect(),
        Some(v) => v.split(',').collect(),
        None => Vec::new(),
    };

    // Dropping every directive leaves an empty string, which `EnvFilter` reads
    // as "enable nothing" rather than "unset" — fall back to the default.
    let sanitized = retained.join(",");
    let mut env_filter = Some(sanitized.as_str())
        .filter(|s| !s.trim().is_empty())
        .and_then(|v| EnvFilter::try_new(v).ok())
        .unwrap_or_else(|| "info".into());

    if !allow_plan_value_logging {
        for prefix in PLAN_VALUE_TARGET_PREFIXES {
            // Anything the operator set for this exact target survived the
            // filter above, so it is already at or above the floor — adding the
            // floor would *raise* it (an explicit `datafusion=off` would come
            // back as `info`, since `add_directive` replaces by target).
            if retained.iter().any(|d| directive_target(d) == Some(prefix)) {
                continue;
            }
            env_filter = env_filter.add_directive(
                format!("{prefix}=info")
                    .parse()
                    .expect("valid plan-value floor directive"),
            );
        }
    }

    if !rust_log.is_some_and(|v| v.contains("aws_config")) {
        env_filter = env_filter.add_directive("aws_config=warn".parse().expect("valid directive"));
    }
    env_filter
}

/// Read the plan-logging opt-out from the environment.
pub fn allow_plan_value_logging_from_env() -> bool {
    std::env::var(ALLOW_PLAN_VALUE_LOGGING_ENV).is_ok_and(|v| {
        !matches!(
            v.trim().to_ascii_lowercase().as_str(),
            "" | "0" | "false" | "no" | "off"
        )
    })
}

/// Whether one comma-separated `RUST_LOG` directive would enable DEBUG/TRACE
/// output for a plan-printing target.
///
/// Bare level names (`debug`) set the *global default* and are left alone — the
/// per-target floor directives out-specify them. A bare target (`datafusion`)
/// means TRACE for that target in env_logger syntax, so it counts as a leak.
fn directive_leaks_plan_values(directive: &str) -> bool {
    let Some(target) = directive_target(directive) else {
        return false;
    };
    if !PLAN_VALUE_TARGET_PREFIXES
        .iter()
        .any(|prefix| target.starts_with(prefix))
    {
        return false;
    }

    let level = directive
        .trim()
        .rsplit_once('=')
        .map(|(_, level)| level.trim());
    match level {
        // `datafusion` / `datafusion=` both mean TRACE.
        None | Some("") => true,
        Some(level) => {
            let level = level.to_ascii_lowercase();
            level == "debug" || level == "trace" || level.parse::<u8>().is_ok_and(|n| n >= 4)
        }
    }
}

/// The target a directive names, with any span filter stripped
/// (`datafusion[span{k=v}]=debug` -> `datafusion`). `None` for a bare level
/// name, which sets the global default rather than naming a target.
fn directive_target(directive: &str) -> Option<&str> {
    let directive = directive.trim();
    if directive.is_empty() {
        return None;
    }
    let target = match directive.rsplit_once('=') {
        Some((target, _)) => target,
        None if is_level_name(directive) => return None,
        None => directive,
    };
    Some(target.split('[').next().unwrap_or(target).trim())
}

/// Whether a bare `RUST_LOG` token is a level (global default) rather than a
/// target name.
fn is_level_name(token: &str) -> bool {
    matches!(
        token.to_ascii_lowercase().as_str(),
        "off" | "error" | "warn" | "info" | "debug" | "trace"
    ) || token.parse::<u8>().is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn env_filter_defaults_to_info_and_caps_aws_config() {
        let filter = build_env_filter(None, false).to_string();
        assert!(filter.contains("info"), "got {filter}");
        assert!(filter.contains("aws_config=warn"), "got {filter}");
    }

    #[test]
    fn env_filter_caps_aws_config_for_unrelated_rust_log() {
        let filter = build_env_filter(Some("debug"), false).to_string();
        assert!(filter.contains("debug"), "got {filter}");
        assert!(filter.contains("aws_config=warn"), "got {filter}");
    }

    #[test]
    fn env_filter_honors_explicit_aws_config_opt_in() {
        let filter = build_env_filter(Some("info,aws_config=debug"), false).to_string();
        assert!(filter.contains("aws_config=debug"), "got {filter}");
        assert!(!filter.contains("aws_config=warn"), "got {filter}");
    }

    #[test]
    fn env_filter_falls_back_to_info_on_invalid_rust_log() {
        let filter = build_env_filter(Some("not a [valid directive"), false).to_string();
        assert!(filter.contains("info"), "got {filter}");
        assert!(filter.contains("aws_config=warn"), "got {filter}");
    }

    #[test]
    fn global_debug_does_not_lower_plan_targets() {
        let filter = build_env_filter(Some("debug"), false).to_string();
        for prefix in PLAN_VALUE_TARGET_PREFIXES {
            assert!(filter.contains(&format!("{prefix}=info")), "got {filter}");
        }
    }

    #[test]
    fn targeted_plan_debug_directives_are_dropped() {
        let filter = build_env_filter(
            Some(
                "info,datafusion_optimizer=debug,datafusion_tracing=trace,skardi_query_plan=debug",
            ),
            false,
        )
        .to_string();
        assert!(
            !filter.contains("datafusion_optimizer=debug"),
            "got {filter}"
        );
        assert!(!filter.contains("datafusion_tracing=trace"), "got {filter}");
        assert!(!filter.contains("skardi_query_plan=debug"), "got {filter}");
        assert!(filter.contains("datafusion=info"), "got {filter}");
    }

    #[test]
    fn bare_plan_target_directive_is_dropped() {
        // `RUST_LOG=datafusion` means TRACE for that target.
        let filter = build_env_filter(Some("datafusion"), false).to_string();
        assert!(filter.contains("datafusion=info"), "got {filter}");
        assert!(filter.contains("info"), "got {filter}");
    }

    #[test]
    fn plan_only_rust_log_falls_back_to_info() {
        let filter = build_env_filter(Some("datafusion=debug"), false).to_string();
        assert!(filter.contains("info"), "got {filter}");
    }

    #[test]
    fn non_plan_targets_keep_their_debug_level() {
        let filter = build_env_filter(Some("warn,skardi_server=debug"), false).to_string();
        assert!(filter.contains("skardi_server=debug"), "got {filter}");
    }

    #[test]
    fn raising_plan_targets_is_still_allowed() {
        // Only *lowering* is blocked; `off`/`warn` are honored as written.
        let filter = build_env_filter(Some("debug,datafusion=off"), false).to_string();
        assert!(filter.contains("datafusion=off"), "got {filter}");
    }

    #[test]
    fn explicit_opt_in_lifts_the_floor() {
        let filter = build_env_filter(Some("datafusion_optimizer=debug"), true).to_string();
        assert!(
            filter.contains("datafusion_optimizer=debug"),
            "got {filter}"
        );
        assert!(!filter.contains("datafusion=info"), "got {filter}");
    }

    #[test]
    fn span_filter_syntax_is_recognised() {
        assert!(directive_leaks_plan_values("datafusion[span{k=v}]=debug"));
        assert!(!directive_leaks_plan_values("datafusion[span{k=v}]=info"));
    }
}
