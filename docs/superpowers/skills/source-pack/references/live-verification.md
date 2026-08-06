# Phase 4 — Real-data integration verification

Phase 1 reconciles the pack against the gateway's **declared** contract.
This phase reconciles it against the **wire** — real rows from a real
provider account — and it is not optional for a pack that maps
passthrough rows. The Notion pack (PR #177) is the precedent for why:
it passed every contract-level check (fingerprints pinned, credential
wall reached, 260+ tests green) and still shipped three defects only
real rows could reveal, all flagged by review:

- The captured contract declared `archived` on pages; the live wire
  spells it `is_archived`. The mapped column would have been
  **always-NULL forever, silently** — passthrough columns ride
  `additionalProperties`, so no registration error and no scan error
  ever fires.
- The contract *under*-declared data sources: real rows carry
  `created_time` / `last_edited_time` / `public_url` / `in_trash` that
  the declared item schema never mentions.
- A pinned enum input (`filter.value: data_source`) was valid only
  because of the provider API version the gateway pins
  (`Notion-Version: 2026-03-11`; older versions spell it `database`) —
  nothing at contract level proves such a pin returns rows.

The rule this phase enforces: **declared output schemas are fingerprint
input, not column truth.** For passthrough executors the declared schema
can omit, misname, or `anyOf`-hide the real fields; only live rows
settle a column set. (Coverage caveat: the fingerprint gate protects
only DECLARED properties, and the coverage walker does not descend
`anyOf` branches — a mis-declared column in that gap raises no error at
any stage. The coverage-gap pin tells you exactly which columns live in
the gap and therefore which ones only this phase can verify.)

## 0. Prerequisites — credentials stay out of your hands

Row-level verification needs a real provider account configured in the
gateway. A free-tier / personal test account is fine and reviewers here
accept it explicitly.

- **Ask the user to create the credential and configure it themselves**
  (`PUT /api/connections/<service>` with `{authType, values}` — find
  the exact field names in the provider's `definition.ts` and
  `examples/local-http/<service>.ts`). Give them the exact `curl` with
  the secret read from *their* environment variable. Never accept,
  echo, or store the credential yourself; if the user pastes a secret
  into the conversation anyway, tell them to rotate it after testing.
- Admin API (`/api/*`) needs `OOMOL_CONNECT_ADMIN_TOKEN` set on the
  gateway (runtime token alone gets 401 on admin routes).
- The gateway's credential verification performs a real egress call
  with an SSRF guard that rejects private/reserved resolved IPs.
  Fake-IP DNS environments (e.g. Clash TUN, 198.18/15) fail here with
  "request URL must not resolve to private or reserved IP addresses" —
  the fix is on the user's side (e.g. add the provider's API domain to
  the proxy's fake-ip-filter); do not patch the guard.
- Ask the user to seed the account with a little real data shaped like
  the tables (e.g. for Notion: share a page and a database with the
  integration).

### OAuth providers: the credential is a flow, and scopes are not the only gate

For `authTypes: ["oauth2"]` providers the setup is heavier than an API
key, and the Feishu pack's live pass (skardi PR #186) hit every trap in
sequence — expect them:

- **The flow**: the user creates the provider app themselves and
  registers the gateway's redirect URI (`http://localhost:3000/oauth/callback`);
  the user runs `PUT /api/oauth/configs/<service>` with
  `{clientId, clientSecret}` from their env; then
  `POST /api/oauth/authorizations {"service": "<service>"}` returns an
  `authorizationUrl` the USER opens in a browser. You never touch the
  secret or the browser session.
- **The gateway may request an un-satisfiable scope set.** Providers
  that build their OAuth scope list as the union of every action's
  permissions can exceed what any real app enables (Feishu rejects the
  authorize request outright, error 20027). Narrowing may require a
  clearly-marked local patch to the provider definition plus an
  upstream issue — precedent: oomol-lab/open-connector#267.
- **Scopes granted ≠ scopes the API accepts.** A read can 401 with the
  gateway-declared scopes present in the token: the provider error
  often names the ACTUAL required scope, and the gateway's
  `requiredScopes` metadata is then the bug (Feishu's 99991679 named
  `im:message:readonly` while the actions declared `*.get_as_user` —
  oomol-lab/open-connector#268). Read the provider's own error before
  suspecting your pack.
- **Capability gates sit beyond scopes entirely**: app-level abilities
  (Feishu's bot capability, 232025) and tenant-admin approval of
  high-sensitivity permissions are enforced independently of the OAuth
  grant. Budget for console round-trips with the user.
- **Every scope/permission change needs a FRESH token** — a
  user_access_token snapshots its grants at authorization time, so
  re-run the authorization after any change rather than debugging a
  stale token.

## 1. Probe every action with real inputs first

Before involving Skardi, call each chosen action directly
(`POST /v1/actions/<id>`) with exactly the inputs the pack will send —
fixed inputs, filter pins, page size — and record the responses to a
scratch directory (they become the fixture sources in §4). Check:

- every pinned/fixed input **returns rows**, not just HTTP 200 — an
  empty result from a version-coupled enum pin looks identical to a
  healthy empty account unless the user seeded data for it;
- the real row key set, per table (`sorted(row.keys())` over a few
  rows);
- the real pagination envelope: force multi-page with a small page size
  (`pageSize: 1`) and confirm the continuation token appears where the
  pack's `next_cursor_path` / totals path expects it, and terminates on
  the documented spelling;
- **the declared input bounds, at the boundary**: send the pack's exact
  page size and the schema's declared maximum — declared caps can
  exceed the wire's (Feishu's `im/v1/messages` declares 100, hard-fails
  above 50 with 99992402; skardi PR #186 shipped the corrected 50, and
  oomol-lab/open-connector#269/#271 fixed the schema upstream);
- **the termination signal on the REAL final page**: fetch to the end,
  then deliberately follow whatever token the last page returns. Some
  providers answer the final page with `has_more: false` beside a
  NON-empty token (Feishu wiki's `"0||…"`), so null-token termination
  refetches a finished scan and trips the loop guard — declare
  `has_more_path` when the envelope carries an authoritative has-more
  boolean, and pin the live shape with an e2e
  (oomol-lab/open-connector#270 tracks the upstream normalization).

## 2. Diff real rows against the mapped columns — both directions

For each table, compare the live key set with the pack's columns:

- **Mapped but absent on the wire** → the column would be always-NULL:
  either the wire spells it differently (map the real spelling) or the
  field does not exist on this object type (drop the column). Check
  near-miss spellings explicitly (`archived` vs `is_archived`,
  snake/camel variants).
- **On the wire but unmapped** → decide deliberately: map it if it is
  stable and useful, or leave it with a module-doc rationale (opaque
  payloads, privacy-sensitive fields). "The contract didn't declare it"
  is not a reason — passthrough delivers it regardless.
- Where the wire contradicts the captured contract, **the wire wins**
  for column sets; the contract still drives the fingerprint pin.
  Record the contradiction in the module doc so the next reviewer knows
  it was seen, not missed.
- If the provider versions its API and the gateway pins a version
  header, record the pinned version (grep the executor source) in the
  pack doc and module doc — it explains version-coupled spellings and
  will date any future drift.

## 3. Scan every table end-to-end through skardi-server

Contract probes bypass Skardi; this step proves the pack itself —
registration (fingerprint gate against live discovery), input
generation, pagination, conversion — against the real gateway:

```bash
cargo build -p skardi-server -p skardi-cli
OPEN_CONNECTOR_TOKEN=<runtime-token> ./target/debug/skardi-server \
  --ctx <scratch>/ctx.yaml --port 8087 &
./target/debug/skardi query --server http://localhost:8087 \
  -e "SELECT ... FROM <gateway>.<binding>.<table>"
```

with a ctx binding every table (and real resource values, e.g. a real
page id for a required `blockId`). For each table verify:

- registration succeeds (the fingerprint gate passing against LIVE
  discovery, not a mock);
- real rows come back and **every mapped column produces a non-NULL
  value somewhere** in a seeded dataset. An all-NULL column across data
  you know exists is the always-NULL bug — find the real spelling;
- value-level extraction is right, not just non-NULL: timestamps parse
  from the real spelling, list-plucking columns (rich-text titles)
  yield the expected strings, booleans carry real values;
- required resources forward verbatim; `LIMIT` stops pagination early.

## 4. Re-derive the fixtures from the live captures

Hand-written fixtures encode the same assumptions as the code they
test; after this phase they must be **redacted live captures** instead
— real envelope keys, real field presence/absence per row, real
timestamp spellings, real URL shapes. Redaction methodology:

- map every real UUID to a synthetic one (deterministic counter keeps
  cross-references intact);
- replace names, titles, free text, emails, avatar/file URLs with
  placeholders; keep provider URL *shapes* with synthetic tails;
- replace workspace-specific short ids (e.g. property ids) with
  `prop1`-style tokens;
- keep structural enums, discriminators, timestamps, booleans verbatim;
- **audit the result mechanically**: walk every string value and
  require it to match an allowlist (placeholders, synthetic UUIDs,
  timestamps, known structural enums). Anything that survives the
  filter gets looked at by eye. Real page titles hiding inside URL
  slugs are exactly what this catches.
- **decode one level deeper**: any string value that itself parses as
  JSON (Feishu's `body.content`, Slack blocks) must be decoded and its
  string leaves run through the SAME allowlist. The Feishu round-2
  blocker was exactly this: real member names survived inside a
  JSON-encoded payload the outer-tree audit treated as one opaque
  string.
- **ship the audit as a tripwire test**, not a one-off pass: an
  in-repo test that re-walks every fixture (including the nested
  decode) so the redaction guarantee is enforced by CI, not by memory.
  A cheap broad net helps too — e.g. "no CJK text in any fixture" when
  the workspace's real names share a script no placeholder uses.
- **coarsen capture timestamps** when rows encode person-linked events
  (joins, messages, task completions): zero the trailing digits so the
  instant stops being correlatable while magnitude and ordering (and
  any digit-count-sensitive parsing) survive. Update tests that pinned
  exact values.
- **verify redaction self-consistency**: the deterministic counter only
  helps if cross-references actually still line up — after redacting,
  check that ids repeated across fields (a task's `url` embedding its
  own `guid`) still match, and that provider-unique ids stayed unique.
  A fixture whose value is "internally consistent live capture" must
  survive its own cross-references.
- deliberately-broken fixtures (the admission gate's schema-mismatch
  case) stay synthetic — say so in a comment.

**If PII ever lands in a commit**: fixing the tip is not enough — the
names remain in every earlier commit. Rewrite the branch history
(squash/amend and force-push) so no reachable commit carries them, and
say so in the review reply.

Then update the fixture-driven tests to assert the live shapes, and the
coverage-gap pins if columns moved.

## 5. Record the evidence

The PR must let a reviewer see the verification without re-running it:
per-table live results (row counts, which pinned filters returned rows,
which columns carried real values), the pinned provider API version,
what the live pass CHANGED (renamed/dropped/added columns and why), and
what remains outside the fingerprint gate. Upstream gateway defects the
pass uncovered get FILED as issues on the gateway repo and linked from
the pack doc — findings that live only in a PR body get lost. Put the durable facts in the
module doc and pack doc; put the run evidence in the PR description or
a comment. Stop the gateway and skardi-server when done, and remind the
user to rotate any credential that was exposed.
