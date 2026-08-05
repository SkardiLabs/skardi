//! Egress policy seam for the RSS fetcher.
//!
//! Feed URLs are agent-authored, i.e. attacker-influenceable, so *where* a
//! fetch may connect is a policy decision. Skardi OSS does not make that
//! decision: it ships only [`AllowAll`], and the fetcher reaches any address
//! the host can route to (including link-local `169.254.169.254` and RFC 1918
//! targets). An operator — or Skardi Cloud — supplies a real [`EgressPolicy`]
//! through the fetcher's constructor to restrict egress; the reserved-range
//! taxonomy that would refuse loopback/link-local/private/CGNAT/unique-local
//! targets is Cloud policy, specified in
//! `docs/superpowers/specs/2026-08-03-rss-cloud-egress-design.md`, not shipped
//! here.
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
        if let Err(reason) = policy.check_ip(addr.ip()) {
            return Err(EgressDenied {
                host: host.to_string(),
                ip: addr.ip(),
                reason,
            });
        }
    }
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

    /// Test-only denying policy: refuses every address. OSS ships no such
    /// policy — the seam exists so a caller can inject one.
    #[derive(Debug)]
    struct DenyAll;
    impl EgressPolicy for DenyAll {
        fn check_ip(&self, _ip: IpAddr) -> Result<(), EgressReason> {
            Err("test-denied".into())
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
