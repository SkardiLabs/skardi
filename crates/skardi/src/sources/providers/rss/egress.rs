//! Default-deny egress policy for the RSS fetcher (SSRF guard).
//!
//! Feed URLs are agent-authored configuration — the design's skill layer
//! manages subscriptions from what an agent reads on the web — so a
//! subscription URL is attacker-influenceable input. A prompt-injected agent
//! can add e.g. `http://169.254.169.254/latest/meta-data/...` as a "feed"
//! and have the server fetch cloud instance metadata on its behalf (the
//! design spec names this address as the reason link-local is refused; see
//! `docs/superpowers/specs/2026-07-22-rss-feed-support-design.md`'s
//! "Security" section and its Fetcher paragraph). No existing helper in this
//! repo defends against it — the `llm_extract` image fetch gates by URL
//! scheme and an opt-in flag, never by resolved address — so this module is
//! new logic, kept local to the fetcher's single choke point.
//!
//! ## Why the policy lives in the resolver, not a pre-connect check
//!
//! The naive defense — resolve the host, inspect the address, then hand
//! reqwest the *hostname* to connect — leaves a window open: nothing stops
//! the attacker's DNS from answering differently the second time reqwest
//! itself resolves the name to actually connect (DNS rebinding). Closing
//! that window is structural, not a matter of sequencing two calls
//! correctly: [`PolicyDns`] implements [`reqwest::dns::Resolve`] itself, so
//! the only addresses reqwest ever sees already passed [`EgressPolicy::check_ip`]
//! — there is no second, independent resolution left for an attacker to
//! race, and pooled connections only ever reuse sockets that were validated
//! this way.
//!
//! ## Why a mixed answer fails the whole lookup
//!
//! One hostname can resolve to several addresses. If a lookup returns one
//! public and one private address, filtering down to just the public one
//! and proceeding would still let an attacker-controlled DNS answer pass
//! this check while leaving the private address free to be used by
//! whatever happens next (a retry, a future cache, a different code path
//! that redoes the lookup) — the check would have validated an address
//! nobody is actually required to connect to. [`check_addrs`] therefore
//! fails the *entire* resolution the moment any single returned address is
//! blocked, rather than narrowing to the addresses that passed.
//!
//! ## Why the test seam cannot become a production escape hatch
//!
//! The mock HTTP servers this repo's tests spin up bind `127.0.0.1`, which
//! [`EgressPolicy::default_deny`] refuses like any other loopback address.
//! [`EgressPolicy::allowing_loopback_for_tests`] exists so tests can reach
//! those servers, but it is `pub(crate)` — unreachable from outside this
//! crate — and relaxes *only* the `Loopback` verdict: private, link-local,
//! CGNAT, and unique-local addresses stay refused exactly as under
//! `default_deny`. Production code has exactly one constructor,
//! `default_deny`; there is no config field, environment variable, or
//! builder flag that reaches a private target — the design spec is explicit
//! that no such opt-in ships initially (see its "Future Extensions").

use std::fmt;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::Arc;

use reqwest::dns::{Addrs, Name, Resolve, Resolving};

/// The reserved address range a target was refused for.
///
/// Variant names and [`BlockedRange::as_str`]'s kebab-case strings are a
/// contract, not incidental: both are read back out through
/// [`EgressBlocked`]'s `Display` string, which later tasks store verbatim as
/// `feeds.last_error` and match a substring of in integration tests.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BlockedRange {
    Loopback,
    LinkLocal,
    Private,
    Cgnat,
    UniqueLocal,
    Unspecified,
    Multicast,
    Broadcast,
    Documentation,
}

impl BlockedRange {
    pub fn as_str(&self) -> &'static str {
        match self {
            BlockedRange::Loopback => "loopback",
            BlockedRange::LinkLocal => "link-local",
            BlockedRange::Private => "private",
            BlockedRange::Cgnat => "cgnat",
            BlockedRange::UniqueLocal => "unique-local",
            BlockedRange::Unspecified => "unspecified",
            BlockedRange::Multicast => "multicast",
            BlockedRange::Broadcast => "broadcast",
            BlockedRange::Documentation => "documentation",
        }
    }
}

/// A default-deny egress policy: an [`IpAddr`] either names one of the
/// reserved [`BlockedRange`]s or it is treated as an ordinary public
/// address.
///
/// There is exactly one production constructor, [`EgressPolicy::default_deny`].
/// [`EgressPolicy::allowing_loopback_for_tests`] is `pub(crate)`, so it can
/// never be reached from outside this crate, and relaxes only the
/// `Loopback` verdict — see the module doc.
#[derive(Debug)]
pub struct EgressPolicy {
    allow_loopback: bool,
}

impl EgressPolicy {
    /// The only production policy: every reserved range refused, no
    /// exceptions.
    pub fn default_deny() -> Self {
        Self {
            allow_loopback: false,
        }
    }

    /// Test-only: lets loopback targets through so tests can point at a
    /// locally bound mock server, while every other reserved range —
    /// private/CGNAT/unique-local in particular, i.e. anything an attacker
    /// might plausibly reach on a real deployment's network — stays
    /// refused exactly as under `default_deny`. `pub(crate)` keeps this
    /// unreachable outside the crate; production code has no way to loosen
    /// `default_deny`.
    pub(crate) fn allowing_loopback_for_tests() -> Self {
        Self {
            allow_loopback: true,
        }
    }

    /// Classify `ip` against every reserved range, refusing it unless it is
    /// an ordinary public address.
    ///
    /// IPv4-mapped IPv6 addresses (`::ffff:a.b.c.d`) are unmapped to their
    /// embedded IPv4 form *before* classification, so e.g. `::ffff:10.0.0.1`
    /// is caught as `Private` rather than sliding through as an
    /// unremarkable-looking IPv6 address that no v6 check flags.
    ///
    /// Deliberately does not use `IpAddr::is_global()` — unstable on stable
    /// rustc — so every range is an explicit, stable-only check; the ranges
    /// the standard library has no predicate for (CGNAT, the v4
    /// documentation blocks, v6 link-local/unique-local) are computed from
    /// their defining prefixes instead.
    pub fn check_ip(&self, ip: IpAddr) -> Result<(), BlockedRange> {
        let ip = match ip {
            IpAddr::V6(v6) => match v6.to_ipv4_mapped() {
                Some(v4) => IpAddr::V4(v4),
                None => IpAddr::V6(v6),
            },
            v4 => v4,
        };

        match ip {
            IpAddr::V4(v4) => self.check_v4(v4),
            IpAddr::V6(v6) => self.check_v6(v6),
        }
    }

    fn check_v4(&self, ip: Ipv4Addr) -> Result<(), BlockedRange> {
        if ip.is_loopback() {
            return self.loopback_verdict();
        }
        if ip.is_link_local() {
            return Err(BlockedRange::LinkLocal);
        }
        if ip.is_private() {
            return Err(BlockedRange::Private);
        }
        // RFC 6598 100.64.0.0/10: shared address space ISPs use for
        // carrier-grade NAT. No stable stdlib predicate exists for it
        // (`Ipv4Addr::is_shared` is unstable), so compute the prefix
        // directly: the top two bits of the second octet must be `01`.
        let octets = ip.octets();
        if octets[0] == 100 && (octets[1] & 0xC0) == 64 {
            return Err(BlockedRange::Cgnat);
        }
        if ip.is_unspecified() {
            return Err(BlockedRange::Unspecified);
        }
        if ip.is_multicast() {
            return Err(BlockedRange::Multicast);
        }
        if ip.is_broadcast() {
            return Err(BlockedRange::Broadcast);
        }
        // RFC 5737 TEST-NET-{1,2,3}: reserved for documentation, never
        // legitimately routable, no stable stdlib predicate.
        if matches!(
            octets,
            [192, 0, 2, _] | [198, 51, 100, _] | [203, 0, 113, _]
        ) {
            return Err(BlockedRange::Documentation);
        }
        Ok(())
    }

    fn check_v6(&self, ip: Ipv6Addr) -> Result<(), BlockedRange> {
        if ip.is_loopback() {
            return self.loopback_verdict();
        }
        if ip.is_unspecified() {
            return Err(BlockedRange::Unspecified);
        }
        if ip.is_multicast() {
            return Err(BlockedRange::Multicast);
        }
        let segments = ip.segments();
        // fe80::/10: link-local, the v6 analogue of 169.254.0.0/16.
        if segments[0] & 0xffc0 == 0xfe80 {
            return Err(BlockedRange::LinkLocal);
        }
        // fc00::/7: unique local, the v6 analogue of RFC 1918 private space.
        if segments[0] & 0xfe00 == 0xfc00 {
            return Err(BlockedRange::UniqueLocal);
        }
        Ok(())
    }

    fn loopback_verdict(&self) -> Result<(), BlockedRange> {
        if self.allow_loopback {
            Ok(())
        } else {
            Err(BlockedRange::Loopback)
        }
    }
}

/// A host was refused because it resolved to a reserved range.
///
/// `Display` renders the exact string later tasks store verbatim as
/// `feeds.last_error` and match a substring of in Task 18's integration
/// tests — see the module doc.
#[derive(Debug)]
pub struct EgressBlocked {
    pub host: String,
    pub ip: IpAddr,
    pub range: BlockedRange,
}

impl fmt::Display for EgressBlocked {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "egress blocked: host '{}' resolves to {} address {}",
            self.host,
            self.range.as_str(),
            self.ip
        )
    }
}

impl std::error::Error for EgressBlocked {}

/// The testable core [`PolicyDns::resolve`] delegates to: check every
/// address a lookup returned against `policy`, and fail the *entire*
/// resolution if any one of them is blocked, rather than filtering down to
/// the addresses that passed — see the module doc for why a mixed
/// public/private answer cannot be resolved by keeping only the public
/// address. On success, returns `addrs` unchanged so the caller connects to
/// exactly what the lookup returned.
pub(crate) fn check_addrs(
    policy: &EgressPolicy,
    host: &str,
    addrs: Vec<SocketAddr>,
) -> Result<Vec<SocketAddr>, EgressBlocked> {
    for addr in &addrs {
        if let Err(range) = policy.check_ip(addr.ip()) {
            return Err(EgressBlocked {
                host: host.to_string(),
                ip: addr.ip(),
                range,
            });
        }
    }
    Ok(addrs)
}

/// A DNS resolver that enforces `policy` structurally: reqwest only ever
/// connects to the addresses this returns, and every one of them has
/// already passed [`EgressPolicy::check_ip`] via [`check_addrs`]. See the
/// module doc for why this must be the resolver itself rather than a
/// pre-connect check.
///
/// **This alone does not cover a feed URL whose host is already an IP
/// literal** (e.g. `http://169.254.169.254/...`). Verified directly against
/// the connector reqwest actually builds on
/// (`reqwest::connect::HttpConnector` is a type alias for
/// `hyper_util::client::legacy::connect::HttpConnector<DynResolver>`,
/// reqwest 0.12.28's `src/connect.rs`): its `call_async` parses the URL
/// host as an `IpAddr` first and, when that succeeds, connects directly —
/// skipping the configured resolver entirely (hyper-util 0.1.20's
/// `src/client/legacy/connect/http.rs`, the `dns::SocketAddrs::try_parse`
/// branch). A `Resolve` impl fundamentally cannot intercept a connection
/// that never resolves a name. Whoever wires this in as
/// `ClientBuilder::dns_resolver` (Task 4) must additionally call
/// [`EgressPolicy::check_ip`] directly whenever a request's host already
/// parses as an `IpAddr`, before dispatching the request — this type only
/// closes the hostname path.
#[derive(Debug)]
pub struct PolicyDns {
    policy: Arc<EgressPolicy>,
}

impl PolicyDns {
    pub fn new(policy: Arc<EgressPolicy>) -> Self {
        Self { policy }
    }
}

impl Resolve for PolicyDns {
    fn resolve(&self, name: Name) -> Resolving {
        let policy = Arc::clone(&self.policy);
        Box::pin(async move {
            let host = name.as_str().to_string();
            let addrs: Vec<SocketAddr> =
                tokio::net::lookup_host((host.as_str(), 0)).await?.collect();
            let validated = check_addrs(&policy, &host, addrs)?;
            Ok(Box::new(validated.into_iter()) as Addrs)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reserved_ranges_are_refused_and_public_allowed() {
        let policy = EgressPolicy::default_deny();
        let blocked: &[(&str, BlockedRange)] = &[
            ("127.0.0.1", BlockedRange::Loopback),
            ("::1", BlockedRange::Loopback),
            ("169.254.169.254", BlockedRange::LinkLocal), // cloud metadata
            ("fe80::1", BlockedRange::LinkLocal),
            ("10.0.0.1", BlockedRange::Private),
            ("172.16.0.1", BlockedRange::Private),
            ("192.168.1.1", BlockedRange::Private),
            ("100.64.0.1", BlockedRange::Cgnat),
            ("fc00::1", BlockedRange::UniqueLocal),
            ("fd12:3456::1", BlockedRange::UniqueLocal),
            ("0.0.0.0", BlockedRange::Unspecified),
            ("224.0.0.1", BlockedRange::Multicast),
            ("255.255.255.255", BlockedRange::Broadcast),
            ("::ffff:10.0.0.1", BlockedRange::Private), // v4-mapped v6 unmapped first
            ("::ffff:127.0.0.1", BlockedRange::Loopback),
        ];
        for (ip, want) in blocked {
            let got = policy.check_ip(ip.parse().unwrap()).unwrap_err();
            assert_eq!(&got, want, "ip {ip}");
        }
        for ip in ["1.1.1.1", "93.184.215.14", "2606:4700:4700::1111"] {
            policy.check_ip(ip.parse().unwrap()).unwrap();
        }
    }

    #[test]
    fn test_policy_allows_loopback_but_still_blocks_private() {
        let policy = EgressPolicy::allowing_loopback_for_tests();
        policy.check_ip("127.0.0.1".parse().unwrap()).unwrap();
        // Asserts the specific range, not just `.is_err()`: the point of
        // this test is that loosening the test policy relaxes exactly the
        // `Loopback` verdict and nothing else, so pinning `Private`
        // specifically is what actually distinguishes it from a policy
        // that (incorrectly) let everything through.
        assert_eq!(
            policy.check_ip("10.0.0.1".parse().unwrap()).unwrap_err(),
            BlockedRange::Private
        );
    }

    #[tokio::test]
    async fn resolver_fails_lookup_when_any_address_is_blocked() {
        // check_addrs is the testable core the Resolve impl delegates to:
        // pub(crate) fn check_addrs(policy, host, addrs: Vec<SocketAddr>) -> Result<Vec<SocketAddr>, EgressBlocked>
        let policy = EgressPolicy::default_deny();
        let mixed = vec![
            "93.184.215.14:0".parse().unwrap(),
            "10.0.0.5:0".parse().unwrap(),
        ];
        let err = check_addrs(&policy, "evil.example", mixed).unwrap_err();
        assert_eq!(err.range, BlockedRange::Private);
        let clean = vec!["93.184.215.14:0".parse().unwrap()];
        assert_eq!(
            check_addrs(&policy, "ok.example", clean.clone()).unwrap(),
            clean
        );
    }

    /// [`EgressBlocked`]'s `Display` string is contractual (see the module
    /// doc and the doc comment on the struct itself): later tasks store it
    /// verbatim as `feeds.last_error` and Task 18 asserts a substring of it.
    /// None of the three tests above exercise `Display` directly, so pin
    /// its exact wording here.
    #[test]
    fn egress_blocked_display_names_host_range_and_ip() {
        let err = EgressBlocked {
            host: "evil.example".to_string(),
            ip: "10.0.0.5".parse().unwrap(),
            range: BlockedRange::Private,
        };
        assert_eq!(
            err.to_string(),
            "egress blocked: host 'evil.example' resolves to private address 10.0.0.5"
        );
    }
}
