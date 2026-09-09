//! The console-brokered acquirer (skardi-cloud design 2026-09-08).
//!
//! The other acquirer in this module tree gets an **ID token** and then does
//! the work itself: list memberships, mint a PAT per workspace, verify, write
//! contexts, and roll the whole thing back on failure. This one is shaped
//! differently, and the difference is not stylistic.
//!
//! Here the console is the identity broker. It authenticates the human with
//! whatever adapter it accepts — email or Google, the CLI never learns which —
//! and skardi-global mints the PAT when the CLI redeems an approval the human
//! already gave. So what arrives is a **credential, not an identity**, and
//! three properties follow:
//!
//! 1. **One workspace.** The consent screen selects exactly one (§11.1), so
//!    `--workspace` and `--all-workspaces` have no meaning on this path. The
//!    caller refuses them rather than ignoring them.
//! 2. **No client id.** That is the whole point: `skardi login
//!    --control-plane <url>` works with nothing else.
//! 3. **Nothing here can revoke.** A PAT authenticates to the *gateway*;
//!    skardi-global's `/v1/*` accepts ID tokens, first-party sessions, and
//!    `dev:` bearers, and its PAT resolution is reached only over the gRPC
//!    directory the gateway uses. So the CLI cannot `DELETE
//!    /v1/me/tokens/{id}` with what it holds. Every place the ID-token flow
//!    would revoke, this one reports the `token_id` and names the console —
//!    which is why the exchange returns that id at all.
//!
//! # Why polling
//!
//! §5: the console is `https:` and a loopback listener is `http:`. Loopback is
//! "potentially trustworthy" today, but Private Network Access is tightening,
//! and a flow whose viability depends on where that lands breaks without a
//! code change. Polling also gives `--no-browser` for free — print the URL,
//! approve it from another machine — which is the headless capability the
//! 08-20 design deferred.
//!
//! # The confirmation code
//!
//! `confirm` is the first six hex characters of `request_id`, and approving
//! REQUIRES it. It is printed here because the human has to retype it in the
//! browser: the pending list under Agent access shows the same prefix and no
//! request id, so an approver has to be looking at this terminal to approve
//! this request. Without that, an attacker could open a request, never visit
//! it, and collect a token belonging to whoever approved it from that list.

use anyhow::{Context, Result, anyhow, bail};
use serde::Deserialize;
use serde_json::{Value, json};
use std::fmt;
use std::time::Duration;

use super::control_plane::Membership;

/// Where the CLI-login routes live on the console.
///
/// Not skardi-global directly: it has no public ingress in any deployment we
/// run, which is the concrete reason this design exists. The console's BFF
/// forwards these two paths with **no session cookie attached** — they are
/// marked session-less in its allowlist precisely so a caller with no cookie
/// can reach them.
const BFF_PREFIX: &str = "/api/global/v1/cli-login";

/// How long to wait between polls of the exchange.
///
/// Flat rather than backing off. The window is five minutes and the human is
/// actively working through a browser at the other end, so the thing being
/// optimised is how quickly the terminal notices they finished, not request
/// volume: a five-minute ceiling at this interval is at most 150 requests
/// against one console.
pub const POLL_INTERVAL: Duration = Duration::from_secs(2);

/// One opened request. `request_id` names it, `confirm` is what the human
/// retypes, and `expires_at` is what the CLI prints so the deadline is theirs
/// rather than a silent timeout.
#[derive(Debug, Deserialize)]
pub struct Opened {
    pub request_id: String,
    pub confirm: String,
    pub expires_at: String,
}

/// What the exchange hands back once a human has approved.
///
/// Carries the raw PAT, so no `#[derive(Debug)]` — see the manual impl below.
pub struct Brokered {
    pub token: String,
    /// The revoke handle. Cannot be used by this CLI (see the module docs), and
    /// is retained anyway: it goes into the context so a later login can name
    /// what it orphaned, and it is printed when the flow has to abandon a
    /// credential it cannot revoke.
    pub token_id: String,
    pub expires_at: Option<String>,
    /// The single workspace the consent screen chose.
    pub workspace: String,
    /// §11.2 — the membership rides along, so the context is written without a
    /// second round trip. Empty is tolerated: `workspace` is authoritative,
    /// and this is what lets the context carry an org slug and a role.
    pub memberships: Vec<Membership>,
}

/// Hand-written, for the same reason `Minted`'s is: the raw token must not be
/// reachable through an `{err:?}` rendering or a stray log line.
impl fmt::Debug for Brokered {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Brokered")
            .field("token", &"(redacted)")
            .field("token_id", &self.token_id)
            .field("expires_at", &self.expires_at)
            .field("workspace", &self.workspace)
            .field("memberships", &self.memberships)
            .finish()
    }
}

/// Open a request. Session-less: this is the call made before anyone has
/// authenticated anything.
///
/// `token_expires_at` carries `--expires`, because the mint happens minutes
/// later in a browser that never sees the flag. The console clamps it to its
/// own ceiling, so what is sent is a preference — asking for longer than
/// policy allows yields a shorter credential, not a failure.
pub async fn open_request(
    http: &reqwest::Client,
    console_base: &str,
    challenge: &str,
    client_desc: &str,
    token_expires_at: &str,
) -> Result<Opened> {
    let url = format!("{}{BFF_PREFIX}/requests", trim(console_base));
    let body = json!({
        "challenge": challenge,
        "client_desc": client_desc,
        "token_expires_at": token_expires_at,
    });
    let value = post(http, &url, body).await?;
    let opened: Opened = serde_json::from_value(value)
        .context("the console's CLI-login response was not the expected shape")?;
    if opened.request_id.is_empty() || opened.confirm.is_empty() {
        bail!("the console opened a login request with no id");
    }
    Ok(opened)
}

/// The URL the human opens. Built here so the page path and the BFF path
/// cannot drift apart across two call sites.
pub fn approval_url(console_base: &str, request_id: &str) -> String {
    format!(
        "{}/cli-login?request_id={}",
        trim(console_base),
        crate::client::encode_component(request_id)
    )
}

/// One poll of the exchange. `Ok(None)` means "approved by nobody yet", which
/// is the ordinary answer and not an error.
///
/// Separated from the loop so the three outcomes — pending, approved, gone —
/// are testable without a clock.
pub async fn poll_once(
    http: &reqwest::Client,
    console_base: &str,
    request_id: &str,
    verifier: &str,
) -> Result<Option<Brokered>> {
    let url = format!("{}{BFF_PREFIX}/exchange", trim(console_base));
    let body = json!({ "request_id": request_id, "verifier": verifier });
    let value = post(http, &url, body).await?;

    match value.get("status").and_then(Value::as_str) {
        Some("pending") => Ok(None),
        Some("approved") => {
            let parsed: ExchangeBody = serde_json::from_value(value)
                .context("the console's CLI-login exchange was not the expected shape")?;
            Ok(Some(Brokered {
                token: parsed.token,
                token_id: parsed.token_id,
                expires_at: parsed.expires_at,
                workspace: parsed.workspace,
                memberships: parsed.memberships,
            }))
        }
        // A status this CLI does not know is a control plane newer than it is.
        // Reported rather than treated as pending, because polling forever on
        // a terminal state is the worse failure.
        other => bail!(
            "the console answered the CLI-login exchange with an unexpected status ({}) — this skardi-cli may be older than the control plane",
            other.unwrap_or("none")
        ),
    }
}

#[derive(Deserialize)]
struct ExchangeBody {
    token: String,
    token_id: String,
    #[serde(default)]
    expires_at: Option<String>,
    workspace: String,
    #[serde(default)]
    memberships: Vec<Membership>,
}

/// Cap on a console response body — the same "something is very wrong" bound
/// the control-plane client uses.
const MAX_BODY_BYTES: usize = 1024 * 1024;

/// One POST, with the console's error envelope mapped.
///
/// The BFF passes skardi-global's body through untouched, so the envelope is
/// the nested `{"error": {"code", "message"}}` the control-plane client
/// already reads. The codes worth naming are mapped here rather than in the
/// caller: `no_such_request` is one answer for expired, unknown, and
/// already-redeemed — deliberately, so the endpoint cannot be used to
/// enumerate live ids — which means the CLI has to translate it into the
/// action that fixes all three.
async fn post(http: &reqwest::Client, url: &str, body: Value) -> Result<Value> {
    let response = http
        .post(url)
        .json(&body)
        .send()
        .await
        .with_context(|| format!("cannot reach the console at {url}"))?;
    let status = response.status();
    let text = read_capped(response, url).await?;

    if status.is_success() {
        if text.trim().is_empty() {
            return Ok(Value::Null);
        }
        return serde_json::from_str(&text)
            .with_context(|| format!("{url} returned a body that is not JSON"));
    }

    let (code, message) = parse_error(&text);
    let code = code.unwrap_or_default();
    // 403 on the console's own proxy, rather than from skardi-global, means
    // the path is not on its allowlist — which is what an older console looks
    // like from here. Worth distinguishing: nothing the user can retype fixes
    // it, and the flag that does work is the one this design replaced.
    if code == "route_not_allowed" {
        bail!(
            "this console does not support browser-brokered `skardi login` (its API allowlist has no CLI-login route). Upgrade the console, or log in with --client-id <OAuth client id>"
        );
    }
    match code.as_str() {
        "no_such_request" => Err(anyhow!(
            "the login request is no longer open — it expired, or it was already redeemed. Run `skardi login` again"
        )),
        "verifier_mismatch" => Err(anyhow!(
            "the console refused this CLI's proof of ownership for the login request — start a fresh `skardi login` rather than reusing a request id"
        )),
        _ if message.is_empty() => Err(anyhow!(
            "the console refused the CLI-login request (HTTP {})",
            status.as_u16()
        )),
        _ => Err(anyhow!(
            "the console refused the CLI-login request: {message} (HTTP {})",
            status.as_u16()
        )),
    }
}

/// `(code, message)` out of the nested envelope, falling back to the raw first
/// line for anything that is not one — a proxy error page, an HTML 502.
fn parse_error(text: &str) -> (Option<String>, String) {
    #[derive(Deserialize)]
    struct Envelope {
        error: ErrorBody,
    }
    #[derive(Deserialize)]
    struct ErrorBody {
        #[serde(default)]
        code: Option<String>,
        #[serde(default)]
        message: Option<String>,
    }
    match serde_json::from_str::<Envelope>(text) {
        Ok(envelope) => (
            envelope.error.code,
            envelope.error.message.unwrap_or_default(),
        ),
        Err(_) => (
            None,
            text.lines().next().unwrap_or_default().trim().to_string(),
        ),
    }
}

async fn read_capped(mut response: reqwest::Response, url: &str) -> Result<String> {
    let mut buf: Vec<u8> = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .with_context(|| format!("read the response from {url}"))?
    {
        if buf.len().saturating_add(chunk.len()) > MAX_BODY_BYTES {
            bail!("{url} returned more than {MAX_BODY_BYTES} bytes — refusing to buffer it");
        }
        buf.extend_from_slice(&chunk);
    }
    Ok(String::from_utf8_lossy(&buf).to_string())
}

fn trim(base: &str) -> &str {
    base.trim_end_matches('/')
}
