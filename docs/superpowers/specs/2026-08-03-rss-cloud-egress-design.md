# RSS Cloud Egress Governance Design

**Status:** Draft for review
**Date:** 2026-08-03
**Scope:** Skardi Cloud (skardi-cloud) — egress/SSRF governance for the RSS provider
**Companion:** [RSS Feed Support Design](2026-07-22-rss-feed-support-design.md) (OSS provider)

## Summary

The OSS RSS provider ships a bare fetcher: it reaches any address the host can route to and exposes an `EgressPolicy` seam whose only OSS implementation is `AllowAll` (no destination filtering — see the OSS design's Security section). OSS deliberately does not sandbox egress, delegating that to the operator. **Skardi Cloud is that operator.** This document specifies the egress governance Cloud layers on top of the OSS seam, in two independent layers:

- **Layer 1 — infrastructure controls.** Deployment-level network isolation that holds regardless of application code: an egress `NetworkPolicy`, IMDSv2 with a hop limit, and minimal IAM scope.
- **Layer 2 — application policy.** A concrete `EgressPolicy` injected into the OSS fetcher through its seam, carrying the reserved-range taxonomy that was removed from OSS, plus per-tenant feed allowlists, fetch quotas, and egress audit logging.

The two layers are defense in depth: Layer 1 contains a fetch that Layer 2 missed (or that a bug bypassed), and Layer 2 gives per-tenant granularity and audit that a blanket network policy cannot express. Neither alone is sufficient.

This document covers **egress only**. Other Cloud differentiators for the RSS provider — multi-tenant fetch quotas beyond SSRF containment, persistent/shared cache, durable storage — are separate designs and are out of scope here except where they intersect egress (per-tenant allowlists and audit, below).

## Motivation

Feed URLs are agent-authored configuration: the `auto_news_base` skill manages subscriptions from what an agent reads on the open web, so a subscription URL is attacker-influenceable input. The concrete attack the OSS provider does not defend against, and that Cloud must:

- A **prompt-injected agent** adds `http://169.254.169.254/latest/meta-data/iam/security-credentials/` as a "feed"; the server fetches instance-metadata credentials on the attacker's behalf.
- A **feed that was legitimate at subscription time** later has its domain expire and get re-registered, or its server compromised, and begins **302-redirecting** to an internal address — a target the subscriber could not have vetted up front.
- **DNS rebinding**: the host resolves to a public address when checked and to `127.0.0.1` when connected, if the check and the connect are two independent resolutions.

In a multi-tenant hosted context these matter more than in single-tenant self-hosting: feed URLs are attacker-influenceable *without* even needing to hijack an agent (a tenant is an untrusted principal), the instance-metadata credentials reachable from a shared node are higher-value, and one tenant's SSRF must never reach another tenant's network segment. OSS's "operator is responsible" boundary lands the whole of this on Cloud.

## Layer 1 — Infrastructure controls

Zero application code. These exist in `deploy/` (which does not yet exist in the repository — this design creates it) and in the Cloud provisioning stack.

- **Egress `NetworkPolicy` (default-deny).** The RSS-serving workload's namespace denies egress to RFC 1918 (`10/8`, `172.16/12`, `192.168/16`), CGNAT (`100.64/10`), link-local (`169.254/16`, including the metadata address), unique-local IPv6 (`fc00::/7`), and loopback, allowing only DNS and public-internet egress. This is the backstop that holds even if Layer 2 has a bug: a fetch that the application policy failed to refuse still cannot leave the namespace toward an internal target.
- **IMDSv2, hop limit 1.** The node's instance-metadata service requires session tokens (IMDSv2) and sets the PUT response hop limit to 1, so a container cannot reach metadata even if it routes a packet at `169.254.169.254`. This closes the single highest-value SSRF target independently of both the network policy and the application policy.
- **Minimal IAM scope.** The node/pod role grants only what the workload needs, so metadata credentials, if ever reached, authorize as little as possible — blast-radius reduction, not prevention.
- **Per-tenant namespace isolation.** Tenants are separated at the network layer so a tenant's egress (or SSRF) cannot reach another tenant's segment.

## Layer 2 — Application policy

Injected into the OSS fetcher through the `EgressPolicy` seam. This is the reserved-range logic deleted from OSS, re-homed in Cloud, plus the per-tenant and audit concerns that are inherently Cloud-only.

- **Reserved-range `EgressPolicy`.** A concrete implementation of the OSS `EgressPolicy` trait that refuses loopback, link-local (incl. `169.254.169.254`), private (RFC 1918), CGNAT, and unique-local addresses. Enforced at the resolver layer via the OSS `PolicyDns` seam so it holds against DNS rebinding: the addresses the fetcher connects to are exactly the ones the policy validated, on the initial URL and on every redirect hop, with no second independent resolution for an attacker to race. A mixed DNS answer (one public, one private address) fails the whole resolution rather than narrowing to the public address. This is the same mechanism the OSS provider once shipped; it lives in Cloud now because *which addresses to refuse* is policy, and policy is what Cloud owns.
- **Per-tenant feed allowlists.** A tenant may be restricted to an explicit set of feed hosts/CIDRs; a subscription outside the allowlist is refused before fetch. This is finer-grained than the blanket reserved-range deny and is meaningless in single-tenant OSS.
- **Fetch quotas and rate limits.** Per-tenant caps on fetch volume and frequency, bounding a hijacked agent's ability to use the fetcher as an amplifier or exfiltration channel. (Broader quota design is a separate document; only the egress-abuse dimension is in scope here.)
- **Egress audit logging.** Every refused fetch — reserved-range hit, allowlist miss, quota trip — is logged with tenant, feed, target, and reason, so an SSRF attempt is an auditable event, not a silent refusal. Successful fetches to unusual destinations can be flagged for review.

## OSS interface contract

Cloud must inject Layer 2 **without forking** the OSS fetcher. OSS therefore guarantees this seam (defined in the OSS provider, `crates/skardi/src/sources/providers/rss/`):

- **`trait EgressPolicy: Send + Sync + Debug`** with `fn check_ip(&self, ip: IpAddr) -> Result<(), EgressReason>`, consulted for every resolved address. The method returns only the *reason* a target is refused (e.g. `"link-local"`); the fetcher pairs that reason with the host and ip into an `EgressDenied`, since `check_ip` sees the address but not the originating host. `EgressReason` is `Cow<'static, str>`. OSS ships `AllowAll` (always `Ok`); Cloud supplies its own implementation.
- **`PolicyDns`** — the fetcher's DNS resolver wraps the injected `EgressPolicy`, so an injected policy is enforced at resolution time (rebinding-safe) with no additional Cloud wiring.
- **Per-redirect re-check** — the fetcher re-runs the policy against each resolved redirect target, so a policy sees redirect hops, not just the initial URL. This is the OSS fetcher's manual redirect loop, which exists regardless of policy.
- **`FetchError::Egress(EgressDenied)`** — a denied fetch surfaces through the existing error path and degrades that feed exactly like an unreachable one (`feeds.last_status = 'error'`, `last_error` names the reason, zero rows in `items`, other feeds unaffected). Cloud reuses this variant rather than adding its own.

The injection point is registration, not the fetcher: the `fetch` module is private, so its constructor is plumbing an embedder never touches. The public seam is `register_rss_tables_with_policy` — `register_rss_tables` with one extra `Arc<dyn EgressPolicy>` argument — which carries the policy to the fetcher internally, and `EgressPolicy`, `EgressReason`, `EgressDenied`, and `AllowAll` are re-exported at the provider root (`skardi::sources::providers::rss`) so the contract is nameable from outside the crate. Cloud registers each `rss` source through that entry point with its own implementation. No OSS source change is required to add Cloud egress governance — only the injected object; `crates/skardi/tests/rss_egress_injection.rs` holds this claim to account from an external crate's position, implementing a denying policy against the public API alone and observing the refusal in `feeds.last_error`.

## Non-goals

- **General multi-tenant quotas** beyond SSRF/egress-abuse containment — a separate quota/limits design.
- **Persistent / shared cache** (serve-stale across restarts) — a separate Cloud caching design; it intersects egress only in that a cache hit avoids a fetch, not in policy.
- **Modifying the OSS default.** OSS stays `AllowAll`; this design adds a Cloud layer, it does not re-introduce a default-on guard into the OSS crate.
- **Full-article extraction egress.** If a future feature fetches `link`/enclosure URLs, it inherits the same seam and the same layered governance; its own design covers it.

## Rollout

- **Layer 1 first.** The `NetworkPolicy`, IMDSv2 hop limit, and IAM scoping are the backstop and have no dependency on application code; they land as a `deploy/` / provisioning change and protect even the current `AllowAll` fetcher.
- **Layer 2 with the Cloud RSS offering.** The injected `EgressPolicy`, per-tenant allowlists, quotas, and audit land in skardi-cloud when the RSS provider is offered as a managed feature, wired through the OSS seam.

## Open questions

- Whether per-tenant allowlists default to open (reserved-range deny only) or to a curated public-feed set per tenant tier.
- Where egress audit events land (the run ledger vs. a dedicated security audit sink) and their retention.
- Whether Layer 2's reserved-range policy is maintained in skardi-cloud or published as a reusable crate an OSS operator can also depend on (which would let self-hosters opt into the same policy without it being an OSS default).
