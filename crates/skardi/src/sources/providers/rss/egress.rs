//! Egress policy seam for the RSS fetcher.
//!
//! Feed URLs are agent-authored, i.e. attacker-influenceable, so *where* a
//! fetch may connect is a policy decision. Skardi OSS does not make that
//! decision: it ships only [`AllowAll`], and the fetcher reaches any address
//! the host can route to (including link-local `169.254.169.254` and RFC 1918
//! targets). An operator — or Skardi Cloud — supplies a real [`EgressPolicy`]
//! through the fetcher's constructor to restrict egress; the reserved-range
//! taxonomy that would refuse loopback/link-local/private/CGNAT/unique-local
//! targets is Cloud policy, specified in the RSS Cloud egress design doc
//! (`docs/superpowers/specs/2026-08-03-rss-cloud-egress-design.md`, added
//! later in this stack), not shipped here.
//!
//! The seam is enforced at the DNS-resolver layer ([`PolicyDns`]) so an
//! injected policy holds against DNS rebinding: reqwest only ever connects to
//! addresses that already passed [`EgressPolicy::check_ip`], and a lookup that
//! returns any refused address fails whole (see [`check_addrs`]). A feed URL
//! whose host is already an IP literal never reaches the resolver, so
//! [`super::fetch::FeedFetcher`] additionally calls [`EgressPolicy::check_ip`]
//! on every hop whose host parses as an `IpAddr`. Under the `AllowAll` default
//! both paths are no-ops.

// This module's only consumer today is `fetch.rs`, and `fetch.rs` in turn
// has no production caller yet — the engine (a later PR in this stack) is
// the first one, and hasn't landed. Until then, everything here outside of
// this module's own tests is unreferenced from a build that excludes test
// code, and `cargo check`/`cargo build` would otherwise flag the whole
// file. Remove this once the engine wires `FeedFetcher` in.
#![allow(dead_code)]

use std::borrow::Cow;
use std::error::Error as StdError;
use std::fmt;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;

use reqwest::dns::{Addrs, Name, Resolve, Resolving};

/// Why a target was refused. A policy names the reason (e.g. `"link-local"`);
/// [`FeedFetcher`] pairs it with the host and ip into an [`EgressDenied`]. The
/// string is contractual: it is stored verbatim as `feeds.last_error`.
///
/// [`FeedFetcher`]: super::fetch::FeedFetcher
pub type EgressReason = Cow<'static, str>;

/// Decides whether the fetcher may connect to a resolved address.
///
/// Consulted for every address a feed host resolves to, on the initial URL and
/// on every redirect hop. OSS ships only [`AllowAll`]; Cloud (or an operator)
/// supplies an implementation that refuses reserved ranges.
pub trait EgressPolicy: Send + Sync + fmt::Debug {
    /// `Ok(())` to allow the connection to `ip`, `Err(reason)` to refuse it.
    fn check_ip(&self, ip: IpAddr) -> Result<(), EgressReason>;
}

/// The OSS default: every address is allowed. Skardi OSS does not sandbox
/// fetch egress — see the module doc and the design spec's Security section.
#[derive(Debug, Default)]
pub struct AllowAll;

impl EgressPolicy for AllowAll {
    fn check_ip(&self, _ip: IpAddr) -> Result<(), EgressReason> {
        Ok(())
    }
}

/// A host was refused by the active [`EgressPolicy`]. `Display` is the exact
/// string stored verbatim as `feeds.last_error`.
#[derive(Debug)]
pub struct EgressDenied {
    pub host: String,
    pub ip: IpAddr,
    pub reason: EgressReason,
}

impl fmt::Display for EgressDenied {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "egress blocked: host '{}' resolves to {} address {}",
            self.host, self.reason, self.ip
        )
    }
}

impl StdError for EgressDenied {}

/// Check every address a lookup returned against `policy`, failing the *whole*
/// resolution if any one is refused rather than narrowing to the addresses
/// that passed — a mixed public/private answer must not be resolved by keeping
/// only the public address (see the Cloud egress design). On success returns
/// `addrs` unchanged so the caller connects to exactly what the lookup
/// returned.
pub(crate) fn check_addrs(
    policy: &dyn EgressPolicy,
    host: &str,
    addrs: Vec<SocketAddr>,
) -> Result<Vec<SocketAddr>, EgressDenied> {
    for addr in &addrs {
        // Canonicalize for the CHECK only: a dual-stack connect to an
        // IPv4-mapped v6 (an AAAA answer like `::ffff:10.0.0.1`) reaches the
        // unmapped V4, so the policy must judge that V4 or the mapped form
        // bypasses a V4-private rule. `to_canonical` unmaps mapped-v6 to V4 and
        // leaves everything else unchanged.
        let ip = addr.ip().to_canonical();
        if let Err(reason) = policy.check_ip(ip) {
            return Err(EgressDenied {
                host: host.to_string(),
                ip,
                reason,
            });
        }
    }
    // Return the ORIGINAL addrs: canonicalization gated the check, but the
    // caller must connect to exactly what the lookup returned.
    Ok(addrs)
}

/// A DNS resolver that enforces `policy` structurally: reqwest only connects
/// to the addresses this returns, each already validated by [`check_addrs`].
/// With the [`AllowAll`] default this is a pass-through resolver. See the
/// module doc for why the policy must live in the resolver (DNS rebinding) and
/// why the IP-literal path needs a separate check in the fetcher.
#[derive(Debug)]
pub struct PolicyDns {
    policy: Arc<dyn EgressPolicy>,
}

impl PolicyDns {
    pub fn new(policy: Arc<dyn EgressPolicy>) -> Self {
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
            let validated = check_addrs(policy.as_ref(), &host, addrs)?;
            Ok(Box::new(validated.into_iter()) as Addrs)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;

    /// Test-only denying policy: refuses every address. OSS ships no such
    /// policy — the seam exists so a caller can inject one.
    #[derive(Debug)]
    struct DenyAll;
    impl EgressPolicy for DenyAll {
        fn check_ip(&self, _ip: IpAddr) -> Result<(), EgressReason> {
            Err("test-denied".into())
        }
    }

    /// Test-only policy that denies exactly one V4 address and nothing else.
    /// It cannot match a raw IPv4-mapped v6 (`::ffff:10.0.0.1` arrives as
    /// `IpAddr::V6`), so it only refuses if `check_addrs` canonicalized first —
    /// which is precisely what the mapped-v6 test relies on.
    #[derive(Debug)]
    struct DenyV4(Ipv4Addr);
    impl EgressPolicy for DenyV4 {
        fn check_ip(&self, ip: IpAddr) -> Result<(), EgressReason> {
            if ip == IpAddr::V4(self.0) {
                Err("test-denied".into())
            } else {
                Ok(())
            }
        }
    }

    #[test]
    fn allow_all_permits_every_address() {
        // The OSS default refuses nothing — including addresses a cloud policy
        // would refuse. This pins that OSS ships no destination filtering.
        let policy = AllowAll;
        for ip in ["127.0.0.1", "169.254.169.254", "10.0.0.1", "1.1.1.1"] {
            policy
                .check_ip(ip.parse().unwrap())
                .unwrap_or_else(|_| panic!("AllowAll must permit {ip}"));
        }
    }

    #[test]
    fn allow_all_check_addrs_returns_addrs_unchanged() {
        let policy = AllowAll;
        let addrs: Vec<SocketAddr> = vec!["10.0.0.5:0".parse().unwrap()];
        assert_eq!(check_addrs(&policy, "any", addrs.clone()).unwrap(), addrs);
    }

    #[test]
    fn check_addrs_surfaces_denied_address_with_host_and_reason() {
        let policy = DenyAll;
        let addrs = vec!["10.0.0.5:0".parse().unwrap()];
        let err = check_addrs(&policy, "evil.example", addrs).unwrap_err();
        assert_eq!(err.host, "evil.example");
        assert_eq!(err.ip, "10.0.0.5".parse::<IpAddr>().unwrap());
        assert_eq!(err.reason, "test-denied");
    }

    #[test]
    fn check_addrs_canonicalizes_mapped_ipv6_before_policy() {
        // The resolver/AAAA-record equivalent of the fetcher's mapped-literal
        // case: a lookup answer of `::ffff:10.0.0.1` is a v6 spelling of the V4
        // 10.0.0.1 that a dual-stack connect reaches. DenyV4 refuses only the
        // raw V4, so this passes only because check_addrs canonicalizes before
        // consulting the policy.
        let policy = DenyV4("10.0.0.1".parse().unwrap());
        let addrs: Vec<SocketAddr> = vec!["[::ffff:10.0.0.1]:80".parse().unwrap()];
        assert!(check_addrs(&policy, "evil.example", addrs).is_err());
    }

    #[test]
    fn egress_denied_display_names_host_reason_and_ip() {
        // Contractual: stored verbatim as feeds.last_error. A cloud policy that
        // reports reason "private" produces exactly this string.
        let err = EgressDenied {
            host: "evil.example".to_string(),
            ip: "10.0.0.5".parse().unwrap(),
            reason: "private".into(),
        };
        assert_eq!(
            err.to_string(),
            "egress blocked: host 'evil.example' resolves to private address 10.0.0.5"
        );
    }
}
