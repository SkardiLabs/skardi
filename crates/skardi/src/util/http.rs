//! Shared helpers for reqwest-based clients (providers, model registries).
//!
//! Retry *loops* intentionally stay with their callers: they differ in
//! attempt counts, retryable status sets, idempotency policy, and error
//! taxonomy, and forcing one shape over them would degrade all of them.
//! The retry *primitives* — header parsing and the like — live here once.

use std::time::Duration;

/// Parse a `Retry-After` response header, in either of its two legal forms:
/// delta-seconds (`Retry-After: 120`) or HTTP-date
/// (`Retry-After: Fri, 15 Aug 2026 08:00:00 GMT`), the latter converted to a
/// duration from now — a date already in the past yields `Duration::ZERO`,
/// not `None`, since "you may retry immediately" is what such a header
/// means. The date form matters for the callers that talk to arbitrary
/// third-party hosts (the rss fetcher above all): CDN-fronted origins emit
/// it routinely, and dropping it silently turns "wait a few minutes" into a
/// sub-second backoff retry.
///
/// Returns `None` when the header is absent, non-ASCII, or neither form;
/// callers apply their own fallback and cap.
///
/// The numeric form is covered end-to-end by the Open Connector client tests
/// (`health_429_honors_retry_after`, `execute_429_is_still_retried`), which
/// drive a mock gateway that emits `Retry-After` headers; the date form by
/// the rss fetcher's `retryable_statuses_retry_with_http_date_retry_after`.
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
    let raw = response
        .headers()
        .get(reqwest::header::RETRY_AFTER)?
        .to_str()
        .ok()?
        .trim();
    if let Ok(seconds) = raw.parse::<u64>() {
        return Some(Duration::from_secs(seconds));
    }
    let date = httpdate::parse_http_date(raw).ok()?;
    Some(
        date.duration_since(std::time::SystemTime::now())
            .unwrap_or(Duration::ZERO),
    )
}
