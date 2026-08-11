# RSS Cloud Egress Governance Design

**Status:** Draft for review
**Date:** 2026-08-03
**Scope:** Skardi Cloud (skardi-cloud) — egress/SSRF governance for the RSS provider
**Companion:** [RSS Feed Support Design](2026-07-22-rss-feed-support-design.md) (OSS provider)

## Summary

The OSS RSS provider ships a bare fetcher: it reaches any address the host can route to and exposes an `EgressPolicy` seam whose only OSS implementation is `AllowAll` (no destination filtering — see the OSS design's Security section). OSS deliberately does not sandbox egress, delegating that to the operator. **Skardi Cloud is that operator.** This document specifies the egress governance Cloud layers on top of the OSS seam, in two independent layers:

- **Layer 1 — infrastructure controls.** Deployment-level network isolation that holds regardless of application code: an egress `NetworkPolicy`, IMDSv2 with a hop limit, and minimal IAM scope.
- **Layer 2 — application controls.** Split by the context each control needs: a concrete `EgressPolicy` injected into the OSS fetcher through its seam carries the destination checks (the reserved-range taxonomy that was removed from OSS, plus per-hop host-allowlist enforcement), while allowlist administration, fetch quotas, and egress audit logging live in Cloud's own orchestration layer, which natively holds the tenant and feed context the seam does not carry.

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

## Layer 2 — Application controls

The `EgressPolicy` seam is deliberately narrow: the fetcher consults it with the hostname and resolved address of a connection attempt — nothing else. It carries no tenant or feed identity and no request lifecycle, and it may be consulted several times per fetch (each resolved address, each redirect hop, again on a reconnecting retry), so it cannot count fetches, meter bytes, or attribute an event to a tenant on its own. Layer 2 therefore splits by the context each control needs: destination checks go in the injected policy, and everything that must know *whose* fetch it is, *which* feed, or *how much* was transferred goes in Cloud's orchestration layer — the code that owns tenants, schedules scans, and reads scan results, which needs no OSS hook.

### In the injected `EgressPolicy`

- **Reserved-range refusal.** A concrete implementation of the OSS `EgressPolicy` trait that refuses loopback, link-local (incl. `169.254.169.254`), private (RFC 1918), CGNAT, and unique-local addresses. Enforced at the resolver layer via the OSS `PolicyDns` seam so it holds against DNS rebinding: the addresses the fetcher connects to are exactly the ones the policy validated, on the initial URL and on every redirect hop, with no second independent resolution for an attacker to race. A mixed DNS answer (one public, one private address) fails the whole resolution rather than narrowing to the public address. This is the same mechanism the OSS provider once shipped; it lives in Cloud now because *which addresses to refuse* is policy, and policy is what Cloud owns.
- **Per-hop host allowlisting.** The seam passes the hostname alongside each resolved address (see the interface contract), so a tenant's feed-host allowlist holds on every hop, not only at subscription time — covering the Motivation's re-registered domain, whose redirect to a public but never-vetted host would pass a reserved-range check and is invisible to a subscription-time check. The policy refuses any hop whose host falls outside the tenant's set.

### In the Cloud orchestration layer

No OSS hook is involved in these: the orchestration layer already knows the tenant, the feed, and the scan outcome.

- **Per-tenant feed allowlist administration.** A tenant may be restricted to an explicit set of feed hosts/CIDRs; a subscription outside the allowlist is refused before any fetch, and the same set parameterizes the per-hop policy check above. This is finer-grained than the blanket reserved-range deny and is meaningless in single-tenant OSS.
- **Fetch quotas and rate limits.** Per-tenant caps on fetch volume and frequency, bounding a hijacked agent's ability to use the fetcher as an amplifier or exfiltration channel. Enforced where fetches are initiated — the scan scheduler and per-tenant feed-count caps — not in the policy, which cannot tell a fetch from a retry or a redirect hop. Byte metering has no application seam at all (the policy runs before a connection exists and never sees a response); if per-tenant byte accounting is needed it belongs on Layer 1's network path — see Open questions. (Broader quota design is a separate document; only the egress-abuse dimension is in scope here.)
- **Egress audit logging.** Every refusal is logged with tenant, feed, target, and reason, so an SSRF attempt is an auditable event, not a silent refusal. A policy refusal already surfaces structured through the OSS error path (`EgressDenied { host, ip, reason }` → `FetchError::Egress` → `feeds.last_status = 'error'` with `last_error` naming the reason); the orchestration layer pairs that with the tenant and feed whose scan produced it. Subscription-time allowlist refusals and quota trips are orchestration-layer events and are logged directly. Successful fetches to unusual destinations can be flagged for review.

## OSS interface contract

Cloud must inject Layer 2's policy half **without forking** the OSS fetcher. OSS therefore guarantees this seam (defined in the OSS provider, `crates/skardi/src/sources/providers/rss/`):

- **`trait EgressPolicy: Send + Sync + Debug`** with `fn check(&self, host: &str, ip: IpAddr) -> Result<(), EgressReason>`, consulted for every resolved address together with the hostname that resolved to it (for an IP-literal URL, the literal itself). The host parameter is what lets host-based policy — the per-hop allowlist above — hold on every hop; OSS's own `AllowAll` ignores it. The method returns only the *reason* a target is refused (e.g. `"link-local"`); the fetcher pairs that reason with the host and ip into an `EgressDenied`. `EgressReason` is `Cow<'static, str>`. OSS ships `AllowAll` (always `Ok`); Cloud supplies its own implementation.
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
- **Layer 2 with the Cloud RSS offering.** The injected `EgressPolicy` (reserved ranges, per-hop allowlisting) is wired through the OSS seam; subscription-time allowlist validation, quotas, and audit land in skardi-cloud's orchestration layer. Both halves ship when the RSS provider is offered as a managed feature.

## Open questions

- Whether per-tenant allowlists default to open (reserved-range deny only) or to a curated public-feed set per tenant tier.
- Where egress audit events land (the run ledger vs. a dedicated security audit sink) and their retention.
- Whether Layer 2's reserved-range policy is maintained in skardi-cloud or published as a reusable crate an OSS operator can also depend on (which would let self-hosters opt into the same policy without it being an OSS default).
- Where per-tenant byte accounting lives if it is needed: the application seam runs before a connection exists and never sees response sizes, so metering bytes means either a Layer 1 egress proxy on the network path or a future fetcher-level hook — neither is designed here.
