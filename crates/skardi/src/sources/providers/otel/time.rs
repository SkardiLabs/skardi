//! Time-window resolution for OTEL range queries.
//!
//! The translator (and the `prom_range` / `loki_range` escape-hatch
//! functions) call `resolve_window` to fold a caller-supplied
//! `(start?, end?, step?)` tuple together with the source's configured
//! defaults into a concrete `TimeWindow`.
//!
//! Caller-supplied values override completely (no merging). A window
//! larger than `defaults.max_window` is rejected with
//! `OtelError::WindowTooLarge` *before* any upstream call is made — see
//! `design.md` Decision 7.

use std::time::Duration;

use chrono::{DateTime, Utc};

use super::error::OtelError;

/// Source-level time-window guardrails loaded from `OtelSourceConfig`.
#[derive(Debug, Clone, Copy)]
pub struct TimeDefaults {
    pub default_window: Duration,
    pub default_step: Duration,
    pub max_window: Duration,
}

impl TimeDefaults {
    /// Defaults documented in `design.md` Decision 7 and `tasks.md` 3.1.
    pub const DEFAULT: Self = Self {
        default_window: Duration::from_secs(15 * 60),
        default_step: Duration::from_secs(30),
        max_window: Duration::from_secs(24 * 60 * 60),
    };
}

/// Resolved range over which a backend query will execute.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TimeWindow {
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
    pub step: Duration,
}

impl TimeWindow {
    /// `end - start`, as a `std::time::Duration`. Always non-negative
    /// because `resolve_window` rejects `start > end` upstream.
    pub fn span(&self) -> Duration {
        (self.end - self.start)
            .to_std()
            .unwrap_or(Duration::from_secs(0))
    }
}

/// Resolve the effective `(start, end, step)` for a range query.
///
/// - Missing `caller_end` defaults to `now`.
/// - Missing `caller_start` defaults to `end - defaults.default_window`.
/// - Missing `caller_step` defaults to `defaults.default_step`.
/// - `start > end` → `OtelError::Backend` with a clear message.
/// - `step == 0` → `OtelError::Backend` (zero-step would loop infinitely on the upstream).
/// - `end - start > defaults.max_window` → `OtelError::WindowTooLarge`.
pub fn resolve_window(
    caller_start: Option<DateTime<Utc>>,
    caller_end: Option<DateTime<Utc>>,
    caller_step: Option<Duration>,
    defaults: &TimeDefaults,
    source_name: &str,
) -> Result<TimeWindow, OtelError> {
    resolve_window_at(
        caller_start,
        caller_end,
        caller_step,
        defaults,
        source_name,
        Utc::now(),
    )
}

/// Same as [`resolve_window`] but accepts an explicit `now` — useful for
/// deterministic tests.
fn resolve_window_at(
    caller_start: Option<DateTime<Utc>>,
    caller_end: Option<DateTime<Utc>>,
    caller_step: Option<Duration>,
    defaults: &TimeDefaults,
    source_name: &str,
    now: DateTime<Utc>,
) -> Result<TimeWindow, OtelError> {
    let end = caller_end.unwrap_or(now);

    let start = match caller_start {
        Some(s) => s,
        None => {
            let dw = chrono::Duration::from_std(defaults.default_window).map_err(|e| {
                OtelError::Backend {
                    source_name: source_name.to_string(),
                    message: format!("invalid default_window: {e}"),
                }
            })?;
            end - dw
        }
    };

    if start > end {
        return Err(OtelError::Backend {
            source_name: source_name.to_string(),
            message: format!(
                "range query has start ({start}) after end ({end}); \
                 swap arguments or set them both"
            ),
        });
    }

    let step = caller_step.unwrap_or(defaults.default_step);
    if step.is_zero() {
        return Err(OtelError::Backend {
            source_name: source_name.to_string(),
            message: "range query step is zero; choose a positive interval".to_string(),
        });
    }

    let span = (end - start).to_std().map_err(|e| OtelError::Backend {
        source_name: source_name.to_string(),
        message: format!("non-representable window span: {e}"),
    })?;

    if span > defaults.max_window {
        return Err(OtelError::WindowTooLarge {
            source_name: source_name.to_string(),
            requested: span,
            max: defaults.max_window,
        });
    }

    Ok(TimeWindow { start, end, step })
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn fixed_now() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 5, 13, 12, 0, 0).unwrap()
    }

    fn defaults_15m() -> TimeDefaults {
        TimeDefaults::DEFAULT
    }

    #[test]
    fn all_defaults_produces_last_default_window_until_now() {
        let now = fixed_now();
        let win = resolve_window_at(None, None, None, &defaults_15m(), "prom", now).unwrap();
        assert_eq!(win.end, now);
        assert_eq!(win.span(), Duration::from_secs(15 * 60));
        assert_eq!(win.step, Duration::from_secs(30));
    }

    #[test]
    fn caller_overrides_step_completely() {
        let now = fixed_now();
        let win = resolve_window_at(
            None,
            None,
            Some(Duration::from_secs(5)),
            &defaults_15m(),
            "prom",
            now,
        )
        .unwrap();
        assert_eq!(win.step, Duration::from_secs(5));
    }

    #[test]
    fn explicit_start_and_end_are_honoured() {
        let now = fixed_now();
        let start = now - chrono::Duration::minutes(60);
        let end = now - chrono::Duration::minutes(30);
        let win =
            resolve_window_at(Some(start), Some(end), None, &defaults_15m(), "prom", now).unwrap();
        assert_eq!(win.start, start);
        assert_eq!(win.end, end);
        assert_eq!(win.span(), Duration::from_secs(30 * 60));
    }

    #[test]
    fn start_after_end_is_rejected() {
        let now = fixed_now();
        let start = now;
        let end = now - chrono::Duration::minutes(10);
        let err = resolve_window_at(Some(start), Some(end), None, &defaults_15m(), "prom", now)
            .unwrap_err();
        match err {
            OtelError::Backend { message, .. } => {
                assert!(message.contains("after end"), "got: {message}");
            }
            other => panic!("expected Backend error, got {other:?}"),
        }
    }

    #[test]
    fn zero_step_is_rejected() {
        let now = fixed_now();
        let err = resolve_window_at(
            None,
            None,
            Some(Duration::from_secs(0)),
            &defaults_15m(),
            "prom",
            now,
        )
        .unwrap_err();
        match err {
            OtelError::Backend { message, .. } => assert!(message.contains("zero")),
            other => panic!("expected Backend error, got {other:?}"),
        }
    }

    #[test]
    fn window_exceeding_max_is_rejected_before_upstream_call() {
        let now = fixed_now();
        // 48h requested, max is 24h.
        let start = now - chrono::Duration::hours(48);
        let err = resolve_window_at(Some(start), Some(now), None, &defaults_15m(), "prom", now)
            .unwrap_err();
        match err {
            OtelError::WindowTooLarge {
                source_name,
                requested,
                max,
            } => {
                assert_eq!(source_name, "prom");
                assert_eq!(requested, Duration::from_secs(48 * 60 * 60));
                assert_eq!(max, Duration::from_secs(24 * 60 * 60));
            }
            other => panic!("expected WindowTooLarge, got {other:?}"),
        }
    }

    #[test]
    fn caller_end_only_extends_window_from_default_window_backwards() {
        let now = fixed_now();
        let end = now - chrono::Duration::hours(1);
        let win = resolve_window_at(None, Some(end), None, &defaults_15m(), "prom", now).unwrap();
        // start = end - 15m (default_window applied relative to the
        // caller-supplied end, not relative to `now`).
        assert_eq!(win.end, end);
        assert_eq!(win.span(), Duration::from_secs(15 * 60));
    }
}
