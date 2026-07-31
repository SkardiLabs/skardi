//! Bounded HTTP fetcher for one feed URL: conditional GET, retries, and
//! egress enforcement.
//!
//! ## Why redirects are followed by hand
//!
//! [`FeedFetcher::new`] builds its client with `redirect::Policy::none()`,
//! and [`FeedFetcher::fetch`] drives a manual per-hop loop instead of
//! letting reqwest follow redirects internally. This is not a style
//! preference — see `super::egress`'s doc on [`PolicyDns`] for the full
//! account, verified there against reqwest's and hyper-util's own source.
//! In short: reqwest's connector checks whether a request's host already
//! parses as an `IpAddr` *before* ever consulting the configured resolver,
//! and connects straight there when it does, skipping [`PolicyDns`]
//! entirely — and that bypass applies exactly the same way to a redirect
//! `Location` as it does to the original URL. `PolicyDns` closes the gap for
//! hostnames by construction; it structurally cannot see an IP-literal
//! target, on the initial URL or on any hop. So every redirect target this
//! module resolves is re-run through [`FeedFetcher::check_hop_target`] — the
//! same scheme allowlist and [`EgressPolicy::check_ip`] check the initial
//! URL gets — before the next request is ever built. A hostname target
//! needs no extra help: it re-enters `PolicyDns` like any other name lookup.
//!
//! ## Validators only cover the first hop
//!
//! `If-None-Match`/`If-Modified-Since` are meaningful only against the
//! resource the caller actually cached — the URL passed to
//! [`FeedFetcher::fetch`]. Once a redirect has been followed, the request is
//! for a *different* URL the cache has no validators for, so they are never
//! resent past the first hop, even though each hop still gets its own fresh
//! retry budget (see below).
//!
//! ## The size cap is measured on the decoded stream
//!
//! [`FeedFetcher::new`] enables gzip decoding on the client, and
//! [`FeedFetcher::read_body`] meters `Response::bytes_stream()` — which
//! yields decoded bytes — as it arrives, rather than trusting a
//! `Content-Length` that describes the wire size and would be meaningless
//! as a bound on decoded size for a compressed body.
//!
//! ## Retries
//!
//! Each hop gets its own budget of [`MAX_ATTEMPTS`] tries: `429` and
//! transient `5xx` (`500`/`502`/`503`/`504`) are retried, as are timeouts
//! and other transport errors — an egress refusal is not, since it can
//! never succeed on retry. The wait between attempts is whichever is longer
//! of the response's `Retry-After` and an exponential backoff with jitter
//! (see [`backoff`]), capped at [`MAX_RETRY_WAIT`] either way —
//! `crate::util::http::parse_retry_after`'s own doc contract is that callers
//! apply their own cap, and an uncapped `Retry-After` is a one-header attack:
//! a hostile or misconfigured server parks the whole fetch for however long
//! it names. Redirects are not retries: following one always starts a new
//! hop with a fresh attempt budget.
//!
//! ## An unconditional `304` is a protocol error, not a cache hit
//!
//! `304 Not Modified` is only meaningful as an answer to a conditional
//! request — it tells the caller "the copy behind the validator you sent is
//! still current," which presupposes a validator was actually sent. A `304`
//! answering a hop that sent none (the very first fetch of a feed the cache
//! has nothing for yet, or any hop past the first redirect — see above) has
//! no cached copy for [`FetchOutcome::NotModified`] to refer to, so
//! [`FeedFetcher::attempt_hop`] tracks whether *this* hop's request actually
//! carried `If-None-Match`/`If-Modified-Since` and maps an unconditional
//! `304` to [`FetchError::Status`] instead of silently handing the engine an
//! outcome it cannot honor.

use std::net::IpAddr;
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use reqwest::header::{
    CONTENT_TYPE, ETAG, HeaderName, IF_MODIFIED_SINCE, IF_NONE_MATCH, LAST_MODIFIED, LOCATION,
};
use reqwest::redirect::Policy;
use reqwest::{Response, StatusCode};
use thiserror::Error;
use url::{Host, Url};

use super::egress::{EgressBlocked, EgressPolicy, PolicyDns};
use super::error::RssError;
use crate::util::http::parse_retry_after;

/// Maximum redirect hops [`FeedFetcher::fetch`] follows before returning
/// [`FetchError::TooManyRedirects`]. Each hop gets its own fresh
/// [`MAX_ATTEMPTS`] budget — see the module doc's Retries section.
pub(crate) const MAX_REDIRECT_HOPS: u32 = 5;

/// Maximum attempts (including the first) for one hop before its last
/// error becomes the terminal [`FetchError`] for the whole fetch.
pub(crate) const MAX_ATTEMPTS: u32 = 3;

/// Base delay for the exponential-backoff component of the retry wait —
/// see [`backoff`].
pub(crate) const RETRY_BASE_BACKOFF_MS: u64 = 250;

/// Upper bound on a single wait between attempts, whether the wait came
/// from a response's `Retry-After` or from [`backoff`]. Without this,
/// `Retry-After` is a one-header denial of service:
/// `crate::util::http::parse_retry_after` deliberately applies no cap of
/// its own ("callers apply their own fallback and cap"), and a hostile or
/// merely misconfigured server naming an absurd value would otherwise park
/// the fetch for exactly as long as it names. Mirrors
/// `open_connector/client.rs`'s `MAX_RETRY_WAIT` (same 10s value, same
/// role), which the brief pointed to as this task's precedent.
const MAX_RETRY_WAIT: Duration = Duration::from_secs(10);

/// HTTP statuses a hop retries rather than treating as terminal: rate
/// limiting plus the transient server errors.
const RETRYABLE_STATUSES: [u16; 5] = [429, 500, 502, 503, 504];

/// Conditional-GET validators from a previously cached fetch. Sent as
/// `If-None-Match`/`If-Modified-Since` on the first hop only — see the
/// module doc.
#[derive(Debug, Clone, Default)]
pub struct Validators {
    pub etag: Option<String>,
    pub last_modified: Option<String>,
}

/// The result of one [`FeedFetcher::fetch`] call.
#[derive(Debug)]
pub enum FetchOutcome {
    /// The server confirmed the cached copy is still current (`304`).
    NotModified { http_status: u16 },
    /// A fresh body, bounded by the fetcher's configured byte cap.
    Fetched {
        body: Vec<u8>,
        http_status: u16,
        etag: Option<String>,
        last_modified: Option<String>,
        content_type: Option<String>,
    },
}

/// Errors from [`FeedFetcher::fetch`].
///
/// `Display` strings are contractual: a later task stores them verbatim as
/// `feeds.last_error`, and later tasks' integration tests match a substring
/// of them.
#[derive(Debug, Error)]
pub enum FetchError {
    /// Refused before connecting: the target (the original URL, or a
    /// redirect hop) parsed or resolved to a reserved address.
    #[error("{0}")]
    Egress(#[from] EgressBlocked),

    /// The decoded response body exceeded the configured cap.
    #[error("response exceeded {limit} bytes")]
    TooLarge { limit: u64 },

    /// A request — or the whole hop, after exhausting retries — timed out.
    #[error("request timed out after {seconds}s")]
    Timeout { seconds: u64 },

    /// A terminal HTTP status: either not retryable at all, or retryable
    /// but still failing after [`MAX_ATTEMPTS`] attempts.
    #[error("http status {status}")]
    Status { status: u16 },

    /// Following the next redirect would exceed [`MAX_REDIRECT_HOPS`].
    #[error("too many redirects (limit {hops})")]
    TooManyRedirects { hops: u32 },

    /// The feed URL — or a redirect target — is not a usable `http(s)` URL.
    #[error("invalid feed url: {reason}")]
    InvalidUrl { reason: String },

    /// A connection or I/O failure not otherwise classified, surfaced after
    /// exhausting retries.
    #[error("transport error: {reason}")]
    Transport { reason: String },
}

/// What one hop's attempt loop produced: either the fetch is done, or a
/// redirect `Location` — not yet resolved against the current hop's URL —
/// must be followed next.
enum HopOutcome {
    Done(FetchOutcome),
    Redirect(String),
}

/// Bounded HTTP fetcher for one feed URL.
///
/// One [`FeedFetcher`] owns a single shared `reqwest::Client`, built once at
/// construction with [`PolicyDns`] as its DNS resolver and
/// redirect-following disabled — see the module doc for why
/// [`FeedFetcher::fetch`] drives redirects itself instead.
#[derive(Debug)]
pub struct FeedFetcher {
    http: reqwest::Client,
    policy: Arc<EgressPolicy>,
    request_timeout: Duration,
    max_response_bytes: u64,
}

impl FeedFetcher {
    /// Build the fetcher's one shared client. `policy` is consulted in two
    /// ways: wrapped in a [`PolicyDns`] as the client's DNS resolver (so
    /// every hostname connection is checked structurally, including
    /// pooled-connection reuse), and held directly for the IP-literal
    /// checks [`FeedFetcher::check_hop_target`] runs before the initial
    /// request and before every redirect hop.
    pub fn new(
        policy: Arc<EgressPolicy>,
        request_timeout: Duration,
        max_response_bytes: u64,
        user_agent: String,
    ) -> Result<Self, RssError> {
        let resolver = Arc::new(PolicyDns::new(Arc::clone(&policy)));
        let http = reqwest::Client::builder()
            .dns_resolver(resolver)
            .redirect(Policy::none())
            .gzip(true)
            .timeout(request_timeout)
            .user_agent(user_agent)
            .build()
            .map_err(|e| RssError::HttpClientBuild {
                reason: e.to_string(),
            })?;
        Ok(Self {
            http,
            policy,
            request_timeout,
            max_response_bytes,
        })
    }

    /// Fetch `url`, sending `validators` as conditional-GET headers on the
    /// first hop only. See the module doc for the redirect and retry rules.
    pub async fn fetch(
        &self,
        url: &str,
        validators: Option<&Validators>,
    ) -> Result<FetchOutcome, FetchError> {
        let mut current = self.parse_and_check(url)?;
        let mut redirects_followed: u32 = 0;

        loop {
            let send_validators = if redirects_followed == 0 {
                validators
            } else {
                None
            };
            match self.attempt_hop(&current, send_validators).await? {
                HopOutcome::Done(outcome) => return Ok(outcome),
                HopOutcome::Redirect(location) => {
                    if redirects_followed >= MAX_REDIRECT_HOPS {
                        return Err(FetchError::TooManyRedirects {
                            hops: MAX_REDIRECT_HOPS,
                        });
                    }
                    current = self.resolve_redirect_target(&current, &location)?;
                    redirects_followed += 1;
                }
            }
        }
    }

    /// Parse the feed URL and apply [`FeedFetcher::check_hop_target`] to it
    /// — the same checks every redirect target gets.
    fn parse_and_check(&self, url: &str) -> Result<Url, FetchError> {
        let parsed = Url::parse(url).map_err(|e| FetchError::InvalidUrl {
            reason: format!("'{url}' is not a valid URL: {e}"),
        })?;
        self.check_hop_target(&parsed)?;
        Ok(parsed)
    }

    /// Resolve a `Location` header against the current hop's URL, then
    /// validate the result exactly as the initial URL was validated — the
    /// check the module doc describes: reqwest's connector cannot see an
    /// IP-literal target on its own, on any hop, so every resolved redirect
    /// target is re-checked here before the next request is built.
    fn resolve_redirect_target(&self, current: &Url, location: &str) -> Result<Url, FetchError> {
        let target = current.join(location).map_err(|e| FetchError::InvalidUrl {
            reason: format!(
                "redirect location '{location}' does not resolve against '{current}': {e}"
            ),
        })?;
        self.check_hop_target(&target)?;
        Ok(target)
    }

    /// Scheme allowlist plus, for an IP-literal host, [`EgressPolicy::check_ip`].
    /// A hostname host needs no check here: [`PolicyDns`] (the client's DNS
    /// resolver) validates it structurally when reqwest actually connects.
    fn check_hop_target(&self, url: &Url) -> Result<(), FetchError> {
        if url.scheme() != "http" && url.scheme() != "https" {
            return Err(FetchError::InvalidUrl {
                reason: format!("scheme '{}' is not http or https", url.scheme()),
            });
        }
        let ip = match url.host() {
            Some(Host::Ipv4(v4)) => Some(IpAddr::V4(v4)),
            Some(Host::Ipv6(v6)) => Some(IpAddr::V6(v6)),
            _ => None,
        };
        if let Some(ip) = ip {
            self.policy.check_ip(ip).map_err(|range| EgressBlocked {
                host: ip.to_string(),
                ip,
                range,
            })?;
        }
        Ok(())
    }

    /// Drive one hop's attempt loop: send the request, retrying up to
    /// [`MAX_ATTEMPTS`] times on a retryable status or a retryable
    /// connection failure, and classifying a successful response into the
    /// outer loop's next step.
    async fn attempt_hop(
        &self,
        url: &Url,
        validators: Option<&Validators>,
    ) -> Result<HopOutcome, FetchError> {
        let mut last_err: Option<FetchError> = None;

        for attempt in 0..MAX_ATTEMPTS {
            let mut req = self.http.get(url.clone());
            // Tracked independently of `validators.is_some()`: a `Validators`
            // with both fields `None` attaches no header either, and a `304`
            // is only a cache hit relative to a validator this specific
            // request actually sent — see the module doc.
            let mut sent_validator = false;
            if let Some(v) = validators {
                if let Some(etag) = &v.etag {
                    req = req.header(IF_NONE_MATCH, etag);
                    sent_validator = true;
                }
                if let Some(last_modified) = &v.last_modified {
                    req = req.header(IF_MODIFIED_SINCE, last_modified);
                    sent_validator = true;
                }
            }

            match req.send().await {
                Ok(resp) => {
                    let status = resp.status();
                    if status.as_u16() == 304 {
                        if sent_validator {
                            return Ok(HopOutcome::Done(FetchOutcome::NotModified {
                                http_status: 304,
                            }));
                        }
                        // An unconditional `304` has no validator behind it
                        // to confirm — treat it as the terminal, non-retryable
                        // status it actually is rather than fabricating a
                        // cache hit the caller never asked for.
                        return Err(FetchError::Status { status: 304 });
                    }
                    if status.is_redirection()
                        && let Some(location) = resp.headers().get(LOCATION)
                    {
                        let location = location.to_str().map_err(|e| FetchError::InvalidUrl {
                            reason: format!("redirect Location header is not valid ASCII: {e}"),
                        })?;
                        return Ok(HopOutcome::Redirect(location.to_string()));
                    }
                    if is_retryable_status(status) {
                        let err = FetchError::Status {
                            status: status.as_u16(),
                        };
                        if attempt + 1 >= MAX_ATTEMPTS {
                            return Err(err);
                        }
                        tokio::time::sleep(retry_wait(&resp, attempt)).await;
                        last_err = Some(err);
                        continue;
                    }
                    if status.is_success() {
                        return Ok(HopOutcome::Done(self.read_body(resp).await?));
                    }
                    return Err(FetchError::Status {
                        status: status.as_u16(),
                    });
                }
                Err(e) => {
                    if let Some(blocked) = find_egress_blocked(&e) {
                        return Err(FetchError::Egress(blocked));
                    }
                    let mapped = if e.is_timeout() {
                        FetchError::Timeout {
                            seconds: self.request_timeout.as_secs(),
                        }
                    } else {
                        FetchError::Transport {
                            reason: e.to_string(),
                        }
                    };
                    if attempt + 1 >= MAX_ATTEMPTS {
                        return Err(mapped);
                    }
                    tokio::time::sleep(backoff(attempt)).await;
                    last_err = Some(mapped);
                }
            }
        }

        // Every branch above returns on the final attempt (the
        // `attempt + 1 >= MAX_ATTEMPTS` guards), so this is unreachable in
        // practice. Kept as a returned error rather than `unreachable!()` so
        // a future change to the loop bounds fails a returned error instead
        // of panicking.
        Err(last_err.unwrap_or(FetchError::Transport {
            reason: "exhausted retry attempts without a recorded error".to_string(),
        }))
    }

    /// Capture the validator/content-type headers, then stream the body,
    /// enforcing the byte cap on the *decoded* stream — see the module doc.
    async fn read_body(&self, resp: Response) -> Result<FetchOutcome, FetchError> {
        let http_status = resp.status().as_u16();
        let etag = header_string(&resp, ETAG);
        let last_modified = header_string(&resp, LAST_MODIFIED);
        let content_type = header_string(&resp, CONTENT_TYPE);

        let limit = self.max_response_bytes;
        let mut body: Vec<u8> = Vec::new();
        let mut stream = resp.bytes_stream();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|e| {
                if e.is_timeout() {
                    FetchError::Timeout {
                        seconds: self.request_timeout.as_secs(),
                    }
                } else {
                    FetchError::Transport {
                        reason: format!("failed to read response body: {e}"),
                    }
                }
            })?;
            if body.len() as u64 + chunk.len() as u64 > limit {
                return Err(FetchError::TooLarge { limit });
            }
            body.extend_from_slice(&chunk);
        }

        Ok(FetchOutcome::Fetched {
            body,
            http_status,
            etag,
            last_modified,
            content_type,
        })
    }
}

fn is_retryable_status(status: StatusCode) -> bool {
    RETRYABLE_STATUSES.contains(&status.as_u16())
}

/// `max(Retry-After, backoff)`, capped at [`MAX_RETRY_WAIT`] — see the
/// module doc's Retries section.
fn retry_wait(resp: &Response, attempt: u32) -> Duration {
    let computed = backoff(attempt);
    let wait = match parse_retry_after(resp) {
        Some(from_header) => from_header.max(computed),
        None => computed,
    };
    wait.min(MAX_RETRY_WAIT)
}

/// Exponential backoff — `RETRY_BASE_BACKOFF_MS * 2^attempt` — randomized
/// within +/-50% of that value using the system clock's sub-second
/// nanoseconds as the source of variation, capped at [`MAX_RETRY_WAIT`]. The
/// jitter source is the same one `open_connector/client.rs`'s own backoff
/// helper uses (chosen there, and reused here, to decorrelate concurrent
/// retries without an added randomness dependency); the +/-50% spread itself
/// is wider than that helper's flat 0-100ms addition, per this fetcher's own
/// spec. The cap matters here independent of [`retry_wait`]'s own: `shift`
/// only clamps at 6 (`RETRY_BASE_BACKOFF_MS * 2^6` = 16s), so a future
/// increase to [`MAX_ATTEMPTS`] alone would otherwise be enough to exceed
/// [`MAX_RETRY_WAIT`] without ever touching a `Retry-After` header.
fn backoff(attempt: u32) -> Duration {
    let shift = attempt.min(6);
    let base_ms = RETRY_BASE_BACKOFF_MS.saturating_mul(1u64 << shift);
    let half = base_ms / 2;
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| u64::from(d.subsec_nanos()))
        .unwrap_or(0);
    let span = half.saturating_mul(2).saturating_add(1);
    let jitter = (nanos % span) as i64 - half as i64;
    let wait_ms = (base_ms as i64 + jitter).max(0) as u64;
    Duration::from_millis(wait_ms).min(MAX_RETRY_WAIT)
}

/// Read one header as an owned `String`, or `None` if absent or not valid
/// UTF-8 text.
fn header_string(resp: &Response, name: HeaderName) -> Option<String> {
    resp.headers()
        .get(name)
        .and_then(|v| v.to_str().ok())
        .map(str::to_string)
}

/// Walk a failed request's source chain for an [`EgressBlocked`] that
/// [`PolicyDns`] raised while connecting.
///
/// Verified against the actual stack reqwest 0.12.28 builds on
/// hyper-util 0.1.20: a `send()` failure during connect surfaces as
/// `reqwest::Error` (`Kind::Request`) whose source is the hyper-util legacy
/// client's own `Error` (`ErrorKind::Connect`), whose source is that
/// connector's `ConnectError` (`msg: "dns error"`), whose source is
/// whatever `PolicyDns::resolve`'s future resolved to — our `EgressBlocked`,
/// when that is what made resolution fail. Rather than downcasting at that
/// fixed depth (an implementation detail of a stack this module does not
/// own), this walks `source()` until either a match or the chain ends.
fn find_egress_blocked(err: &reqwest::Error) -> Option<EgressBlocked> {
    let mut source: Option<&(dyn std::error::Error + 'static)> = std::error::Error::source(err);
    while let Some(e) = source {
        if let Some(blocked) = e.downcast_ref::<EgressBlocked>() {
            return Some(EgressBlocked {
                host: blocked.host.clone(),
                ip: blocked.ip,
                range: blocked.range,
            });
        }
        source = e.source();
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sources::providers::rss::egress::BlockedRange;
    use crate::sources::providers::rss::testutil::{MockFeedServer, MockResponse};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Instant;

    /// Loopback-allowing policy, 2s timeout, 1 MiB cap, `skardi-test` UA —
    /// the shared fixture the brief's tests are written against.
    fn test_fetcher() -> FeedFetcher {
        FeedFetcher::new(
            Arc::new(EgressPolicy::allowing_loopback_for_tests()),
            Duration::from_secs(2),
            1024 * 1024,
            "skardi-test".to_string(),
        )
        .expect("build test fetcher")
    }

    #[tokio::test]
    async fn full_fetch_returns_body_and_validators() {
        let server = MockFeedServer::start(|_req| {
            MockResponse::xml("<rss/>")
                .with_header("etag", "\"v1\"")
                .with_header("last-modified", "Mon, 20 Jul 2026 10:00:00 GMT")
        })
        .await;
        let f = test_fetcher();
        let out = f
            .fetch(&format!("{}/feed.xml", server.url()), None)
            .await
            .unwrap();
        match out {
            FetchOutcome::Fetched {
                body,
                http_status,
                etag,
                last_modified,
                content_type,
            } => {
                assert_eq!(body, b"<rss/>");
                assert_eq!(http_status, 200);
                assert_eq!(etag.as_deref(), Some("\"v1\""));
                assert!(last_modified.is_some());
                assert_eq!(content_type.as_deref(), Some("application/xml"));
            }
            other => panic!("expected Fetched, got {other:?}"),
        }
        assert_eq!(
            server.requests()[0].header("user-agent").as_deref(),
            Some("skardi-test")
        );
    }

    #[tokio::test]
    async fn conditional_get_sends_validators_and_maps_304() {
        let server = MockFeedServer::start(|req| {
            if req.header("if-none-match").as_deref() == Some("\"v1\"") {
                MockResponse::status(304)
            } else {
                MockResponse::xml("<rss/>")
            }
        })
        .await;
        let f = test_fetcher();
        let v = Validators {
            etag: Some("\"v1\"".into()),
            last_modified: Some("Mon, 20 Jul 2026 10:00:00 GMT".into()),
        };
        let out = f
            .fetch(&format!("{}/f", server.url()), Some(&v))
            .await
            .unwrap();
        assert!(matches!(
            out,
            FetchOutcome::NotModified { http_status: 304 }
        ));
        let req = &server.requests()[0];
        assert_eq!(req.header("if-none-match").as_deref(), Some("\"v1\""));
        assert_eq!(
            req.header("if-modified-since").as_deref(),
            Some("Mon, 20 Jul 2026 10:00:00 GMT")
        );
    }

    #[tokio::test]
    async fn unconditional_304_without_validators_is_a_status_error() {
        // A first-ever fetch of a feed the cache has nothing for yet: no
        // validators to send, so a `304` cannot mean "still current" — it
        // must surface as the terminal status it is, not a fabricated cache
        // hit `FetchOutcome::NotModified` has no cached body to back up.
        let server = MockFeedServer::start(|_req| MockResponse::status(304)).await;
        let f = test_fetcher();
        let err = f
            .fetch(&format!("{}/f", server.url()), None)
            .await
            .unwrap_err();
        assert!(
            matches!(err, FetchError::Status { status: 304 }),
            "got {err}"
        );
    }

    #[tokio::test]
    async fn validators_are_not_resent_after_redirect() {
        // The module doc devotes a section to "validators cover only the
        // first hop"; pin it directly rather than relying on
        // `redirect_is_followed_and_validated` (which passes no validators
        // at all and so cannot distinguish "never sent" from "suppressed").
        let server = MockFeedServer::start(|req| {
            if req.path == "/moved" {
                assert!(
                    req.header("if-none-match").is_none(),
                    "validators must not be resent past the first hop"
                );
                MockResponse::xml("<rss/>")
            } else {
                MockResponse::status(302).with_header("location", "/moved")
            }
        })
        .await;
        let f = test_fetcher();
        let v = Validators {
            etag: Some("\"v1\"".into()),
            last_modified: None,
        };
        let out = f
            .fetch(&format!("{}/feed.xml", server.url()), Some(&v))
            .await
            .unwrap();
        assert!(matches!(out, FetchOutcome::Fetched { .. }));

        let requests = server.requests();
        assert_eq!(requests.len(), 2);
        assert_eq!(
            requests[0].header("if-none-match").as_deref(),
            Some("\"v1\""),
            "the first hop must still send the validator"
        );
        assert!(
            requests[1].header("if-none-match").is_none(),
            "the redirect hop must not resend it"
        );
    }

    #[tokio::test]
    async fn unconditional_304_after_redirect_is_a_status_error() {
        // The path a hostile server would actually use: unlike
        // `unconditional_304_without_validators_is_a_status_error`, the
        // caller *did* pass validators — hop 0 sends them and gets a
        // redirect, so hop 1 (the one that answers `304`) sends none,
        // because validators do not follow redirects. Same rule, reached
        // from the other direction.
        let server = MockFeedServer::start(|req| {
            if req.path == "/moved" {
                MockResponse::status(304)
            } else {
                MockResponse::status(302).with_header("location", "/moved")
            }
        })
        .await;
        let f = test_fetcher();
        let v = Validators {
            etag: Some("\"v1\"".into()),
            last_modified: None,
        };
        let err = f
            .fetch(&format!("{}/feed.xml", server.url()), Some(&v))
            .await
            .unwrap_err();
        assert!(
            matches!(err, FetchError::Status { status: 304 }),
            "got {err}"
        );
        assert_eq!(server.requests().len(), 2);
    }

    #[tokio::test]
    async fn oversized_body_aborts_with_too_large() {
        // Covers only the uncompressed cap: the gzip-bomb variant needs the
        // pre-compressed fixture Task 17 adds under fixtures/, so that case
        // is Task 18's integration pass, not this task's — see the brief.
        let big = vec![0u8; 2 * 1024 * 1024];
        let server = MockFeedServer::start(move |_req| MockResponse::new(200, big.clone())).await;
        let f = test_fetcher();
        let err = f
            .fetch(&format!("{}/f", server.url()), None)
            .await
            .unwrap_err();
        assert!(
            matches!(err, FetchError::TooLarge { limit: 1_048_576 }),
            "got {err}"
        );
    }

    #[tokio::test]
    async fn redirect_is_followed_and_validated() {
        let server = MockFeedServer::start(|req| {
            if req.path == "/moved" {
                MockResponse::xml("<rss/>")
            } else {
                MockResponse::status(302).with_header("location", "/moved")
            }
        })
        .await;
        let f = test_fetcher();
        let out = f
            .fetch(&format!("{}/feed.xml", server.url()), None)
            .await
            .unwrap();
        assert!(matches!(out, FetchOutcome::Fetched { .. }));
        let requests = server.requests();
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[1].path, "/moved");
    }

    #[tokio::test]
    async fn too_many_redirects_errors() {
        let server = MockFeedServer::start(|_req| {
            MockResponse::status(302).with_header("location", "/next")
        })
        .await;
        let f = test_fetcher();
        let err = f
            .fetch(&format!("{}/start", server.url()), None)
            .await
            .unwrap_err();
        assert!(
            matches!(err, FetchError::TooManyRedirects { hops: 5 }),
            "got {err}"
        );
        assert_eq!(server.requests().len() as u32, MAX_REDIRECT_HOPS + 1);
    }

    #[tokio::test]
    async fn redirect_to_blocked_range_is_refused_before_connect() {
        let server = MockFeedServer::start(|_req| {
            MockResponse::status(302).with_header("location", "http://10.255.255.1/f")
        })
        .await;
        let f = test_fetcher();
        let err = f
            .fetch(&format!("{}/start", server.url()), None)
            .await
            .unwrap_err();
        match err {
            FetchError::Egress(e) => assert_eq!(e.range, BlockedRange::Private, "got {e}"),
            other => panic!("expected Egress, got {other:?}"),
        }
        assert_eq!(
            server.requests().len(),
            1,
            "the blocked redirect target must never be connected to"
        );
    }

    #[tokio::test]
    async fn redirect_to_cloud_metadata_is_refused() {
        // Named separately from the generic private-range case above so a
        // regression here reads as what it is: the address the per-hop
        // IP-literal check exists for in the first place.
        let server = MockFeedServer::start(|_req| {
            MockResponse::status(302).with_header(
                "location",
                "http://169.254.169.254/latest/meta-data/iam/security-credentials/",
            )
        })
        .await;
        let f = test_fetcher();
        let err = f
            .fetch(&format!("{}/start", server.url()), None)
            .await
            .unwrap_err();
        match err {
            FetchError::Egress(e) => assert_eq!(e.range, BlockedRange::LinkLocal, "got {e}"),
            other => panic!("expected Egress, got {other:?}"),
        }
        assert_eq!(server.requests().len(), 1);
    }

    #[tokio::test]
    async fn retryable_statuses_retry_with_retry_after() {
        let calls = Arc::new(AtomicUsize::new(0));
        let calls2 = Arc::clone(&calls);
        let server = MockFeedServer::start(move |_req| {
            if calls2.fetch_add(1, Ordering::SeqCst) == 0 {
                MockResponse::status(429).with_header("retry-after", "1")
            } else {
                MockResponse::xml("<rss/>")
            }
        })
        .await;
        let f = test_fetcher();
        let start = Instant::now();
        let out = f.fetch(&format!("{}/f", server.url()), None).await.unwrap();
        assert!(matches!(out, FetchOutcome::Fetched { .. }));
        assert_eq!(server.requests().len(), 2);
        assert!(
            start.elapsed() >= Duration::from_secs(1),
            "elapsed {:?}, expected the 1s retry-after to be honored",
            start.elapsed()
        );
    }

    #[tokio::test]
    async fn retry_after_is_capped_at_max_retry_wait() {
        // A `Retry-After` naming ~11.5 days is a one-header denial of
        // service if honored literally; the fetch must still complete in
        // well under that, bounded by MAX_RETRY_WAIT rather than the
        // header's value.
        //
        // This is a real-time test, not `#[tokio::test(start_paused =
        // true)]`: tried it first, and it fails spuriously — with time
        // paused, tokio's auto-advance races the virtual clock ahead of the
        // mock server's real socket round-trip for the *first* request,
        // and reqwest's own per-request timeout (also driven by the paused
        // clock) fires before that real response ever arrives, so the test
        // fails with `Timeout { seconds: 2 }` on every run, not just a
        // regression. Two things compensate for going back to real time:
        // an explicit `tokio::time::timeout` around the fetch so a
        // regressed (unclamped) wait fails an assertion in ~20s rather than
        // hanging for ~11.5 days, and a tight elapsed window (rather than a
        // loose "well under 30s") so the assertion actually discriminates
        // the clamped case from an unclamped one instead of just from a
        // hang.
        let calls = Arc::new(AtomicUsize::new(0));
        let calls2 = Arc::clone(&calls);
        let server = MockFeedServer::start(move |_req| {
            if calls2.fetch_add(1, Ordering::SeqCst) == 0 {
                MockResponse::status(429).with_header("retry-after", "999999")
            } else {
                MockResponse::xml("<rss/>")
            }
        })
        .await;
        let f = test_fetcher();
        let start = Instant::now();
        let out = tokio::time::timeout(
            Duration::from_secs(20),
            f.fetch(&format!("{}/f", server.url()), None),
        )
        .await
        .expect(
            "fetch did not complete within 20s — the Retry-After clamp appears to have \
             regressed (an uncapped 999999s wait would hang far longer than this)",
        )
        .unwrap();
        assert!(matches!(out, FetchOutcome::Fetched { .. }));
        assert_eq!(server.requests().len(), 2);
        let elapsed = start.elapsed();
        assert!(
            elapsed >= Duration::from_secs(9) && elapsed <= Duration::from_secs(12),
            "elapsed {elapsed:?}, expected close to MAX_RETRY_WAIT (10s) — \
             a 999999s retry-after honored literally would fail the 20s timeout above instead, \
             but this window is what actually pins the clamp to ~10s rather than merely \"fast\"",
        );
    }

    /// Every status the module doc names as retryable, exercised as a status.
    ///
    /// The list is spelled out here rather than read from `RETRYABLE_STATUSES`,
    /// so a status quietly dropped from that array fails this test instead of
    /// being agreed with — the same discipline `retry_after_is_capped_at_max_retry_wait`
    /// applies to `MAX_RETRY_WAIT`. Before this was parameterised only `429`,
    /// `500` and `503` were ever exercised, and dropping `502` and `504` from the
    /// array left the suite green; `502`/`504` are what a CDN in front of a dead
    /// feed host actually returns. The five literals here and the five the module
    /// doc's Retries section names (`fetch.rs:42-43`) are the same claim, so they
    /// have to move together.
    #[tokio::test]
    async fn retries_exhaust_to_status_error() {
        for status in [429, 500, 502, 503, 504] {
            let server = MockFeedServer::start(move |_req| MockResponse::status(status)).await;
            let f = test_fetcher();
            let err = f
                .fetch(&format!("{}/f", server.url()), None)
                .await
                .unwrap_err();
            match err {
                FetchError::Status { status: got } => assert_eq!(
                    got, status,
                    "the terminal error carries the status that kept failing"
                ),
                other => panic!("expected a Status error for {status}, got {other:?}"),
            }
            assert_eq!(
                server.requests().len() as u32,
                MAX_ATTEMPTS,
                "{status} must be retried to the attempt budget, not treated as terminal"
            );
        }
    }

    #[tokio::test]
    async fn non_retryable_status_fails_immediately() {
        let server = MockFeedServer::start(|_req| MockResponse::status(404)).await;
        let f = test_fetcher();
        let err = f
            .fetch(&format!("{}/f", server.url()), None)
            .await
            .unwrap_err();
        assert!(
            matches!(err, FetchError::Status { status: 404 }),
            "got {err}"
        );
        assert_eq!(server.requests().len(), 1);
    }

    #[tokio::test]
    async fn request_timeout_maps_to_timeout_error() {
        let server = MockFeedServer::start(|_req| {
            MockResponse::xml("<rss/>").with_delay(Duration::from_secs(3))
        })
        .await;
        let f = FeedFetcher::new(
            Arc::new(EgressPolicy::allowing_loopback_for_tests()),
            Duration::from_secs(1),
            1024 * 1024,
            "skardi-test".to_string(),
        )
        .expect("build fetcher");
        let err = f
            .fetch(&format!("{}/f", server.url()), None)
            .await
            .unwrap_err();
        assert!(
            matches!(err, FetchError::Timeout { seconds: 1 }),
            "got {err}"
        );
    }

    #[tokio::test]
    async fn direct_ip_literal_in_blocked_range_is_refused() {
        // Pre-resolution literal check: the target is never connected to
        // (port 9 — the discard service port — is never reached), so there
        // is no mock server to spin up for this test at all.
        let f = test_fetcher();
        let err = f.fetch("http://192.168.0.1:9/f", None).await.unwrap_err();
        match err {
            FetchError::Egress(e) => assert_eq!(e.range, BlockedRange::Private, "got {e}"),
            other => panic!("expected Egress, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn hostname_resolving_to_loopback_is_refused_via_policy_dns() {
        // Every other egress test targets an IP-literal host, caught by
        // `check_hop_target`'s direct `check_ip` call before any connection
        // is attempted — `PolicyDns` (the client's DNS resolver) never gets
        // involved. This drives the other half of the chain: a *hostname*
        // that resolves to a blocked address, recovered from the connect
        // error by `find_egress_blocked`. `localhost` resolves to
        // `127.0.0.1` through the system resolver on every platform this
        // repo already binds mock servers on — no DNS control needed, and
        // no dependency on the mock server actually being reached.
        //
        // Uses `default_deny()`, not the loopback-allowing test policy:
        // loopback must still be refused for a *hostname* target exactly as
        // it is for an IP literal, so this also confirms `PolicyDns` enforces
        // the same policy `check_hop_target` does, not a looser one.
        let server = MockFeedServer::start(|_req| MockResponse::xml("<rss/>")).await;
        let localhost_url = server.url().replace("127.0.0.1", "localhost");
        let f = FeedFetcher::new(
            Arc::new(EgressPolicy::default_deny()),
            Duration::from_secs(2),
            1024 * 1024,
            "skardi-test".to_string(),
        )
        .expect("build fetcher");
        let err = f
            .fetch(&format!("{localhost_url}/f"), None)
            .await
            .unwrap_err();
        match err {
            FetchError::Egress(e) => assert_eq!(e.range, BlockedRange::Loopback, "got {e}"),
            other => panic!("expected Egress, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn https_and_http_only() {
        let f = test_fetcher();
        let err = f
            .fetch("ftp://example.com/feed.xml", None)
            .await
            .unwrap_err();
        assert!(matches!(err, FetchError::InvalidUrl { .. }), "got {err}");
    }

    #[tokio::test]
    async fn invalid_user_agent_fails_client_construction() {
        // RssError::HttpClientBuild: a contractual error variant, reachable
        // from caller-supplied config (RssConfig::validate only checks
        // user_agent is non-empty, not that it survives HeaderValue's
        // stricter validation), otherwise unexercised.
        let err = FeedFetcher::new(
            Arc::new(EgressPolicy::allowing_loopback_for_tests()),
            Duration::from_secs(2),
            1024 * 1024,
            "bad\nua".to_string(),
        )
        .unwrap_err();
        assert!(matches!(err, RssError::HttpClientBuild { .. }), "got {err}");
    }
}
