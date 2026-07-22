//! Shared helpers for reqwest-based clients (providers, model registries).
//!
//! Retry *loops* intentionally stay with their callers: they differ in
//! attempt counts, retryable status sets, idempotency policy, and error
//! taxonomy, and forcing one shape over them would degrade all of them.
//! The retry *primitives* — header parsing and the like — live here once.

use std::time::Duration;

/// Parse a `Retry-After` response header (integer-seconds form).
///
/// Returns `None` when the header is absent, non-ASCII, or not an integer;
/// callers apply their own fallback and cap. The HTTP-date form is not
/// parsed (none of the APIs we integrate with emit it).
///
/// Behavior is covered end-to-end by the Open Connector client tests
/// (`health_429_honors_retry_after`, `execute_429_is_still_retried`), which
/// drive a mock gateway that emits `Retry-After` headers.
///
/// # Example
/// ```
/// # async fn example(resp: reqwest::Response) {
/// use skardi::util::http::parse_retry_after;
/// use std::time::Duration;
///
/// let wait = parse_retry_after(&resp).unwrap_or(Duration::from_secs(2));
/// # }
/// ```
pub fn parse_retry_after(response: &reqwest::Response) -> Option<Duration> {
    response
        .headers()
        .get(reqwest::header::RETRY_AFTER)?
        .to_str()
        .ok()?
        .trim()
        .parse::<u64>()
        .ok()
        .map(Duration::from_secs)
}
