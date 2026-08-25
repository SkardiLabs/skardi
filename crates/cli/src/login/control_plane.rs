//! The skardi-cloud control-plane API `login` and `logout --revoke` speak:
//! `GET /v1/me/workspaces`, `POST /v1/me/tokens`, `DELETE /v1/me/tokens/{id}`.
//!
//! Deliberately NOT [`crate::client::ApiClient`]. That client is for the
//! engine surface, whose error envelope is flat
//! (`{"error": msg, "error_type": tok}`); the control plane nests it
//! (`{"error": {"code": …, "message": …}}`) and adds keys per code — the
//! `org_ambiguous` org list among them (§6.4). One client cannot read both
//! without guessing, and the credential differs too: an ID token (or a `dev:`
//! bearer), never the PAT.
//!
//! The wire shapes here are pinned against skardi-global's handlers, not
//! against prose: `workspaces` returns `{"workspaces": [MembershipView…]}`,
//! and a mint answers `201 {"token": "<raw>", "pat": {"token_id": …}}`.

use anyhow::{Context, Result};
use serde::Deserialize;
use serde_json::{Value, json};
use std::fmt;
use std::time::Duration;

/// Per-request ceiling on control-plane calls.
///
/// Bounded because of the ROLLBACK, not the happy path: a revoke that hangs
/// forever leaves the operator watching a silent terminal while a live
/// credential goes unreported, which is precisely the outcome §6.5 exists to
/// prevent. Generous enough that a cold control plane still answers.
pub const CONTROL_PLANE_TIMEOUT: Duration = Duration::from_secs(30);

/// The HTTP client every control-plane conversation uses — `login`'s and
/// `logout --revoke`'s alike.
///
/// One builder, so a caller cannot acquire an UNBOUNDED client by accident:
/// `logout --revoke` did exactly that, and a stalled `DELETE` would have hung
/// the command after the local credential was already gone. `timeout` is a
/// parameter only so a test can prove the bound bites without waiting
/// [`CONTROL_PLANE_TIMEOUT`] for it.
pub fn client(timeout: Duration) -> Result<reqwest::Client> {
    reqwest::Client::builder()
        .no_proxy()
        .timeout(timeout)
        .build()
        .context("build the HTTP client for the control plane")
}

/// Cap on a control-plane response body. Memberships and one PAT are a few
/// hundred bytes; this is the "something is very wrong" bound, and small
/// enough that a runaway endpoint cannot exhaust memory.
const MAX_BODY_BYTES: usize = 1024 * 1024;

/// One membership as `/v1/me/workspaces` returns it (`MembershipView`).
///
/// `gateway_url` is `Option` PERMANENTLY, not just until the control plane
/// grows it (§7.1): the field is omitted whenever the deployment has no gateway
/// URL configured, and §6.2's precedence then decides — flag, env, or a typed
/// error naming `--server`.
///
/// It is read per MEMBERSHIP, which is the unit that becomes a context. The
/// first control-plane release projects one deployment-wide value onto every
/// membership; a later one supplies each workspace runtime's own endpoint
/// through the same field. Honouring whatever each membership says is what
/// makes the CLI work unchanged across both.
#[derive(Debug, Clone, Deserialize)]
pub struct Membership {
    pub org_slug: String,
    pub tenant_slug: String,
    #[serde(default)]
    pub display_name: Option<String>,
    pub role: String,
    pub provisioning_state: String,
    #[serde(default)]
    pub gateway_url: Option<String>,
}

impl Membership {
    /// `<org_slug>/<tenant_slug>` — the context name §6.1 step 7 writes.
    pub fn context_name(&self) -> String {
        format!("{}/{}", self.org_slug, self.tenant_slug)
    }

    /// Only `active` workspaces can be minted for (§6.1 step 4); `ready` is
    /// not a value skardi-global ever sets.
    pub fn is_active(&self) -> bool {
        self.provisioning_state == "active"
    }
}

/// A freshly minted PAT. The raw token is here and nowhere else until it is
/// written to the config file.
pub struct Minted {
    pub token: String,
    pub token_id: String,
    pub expires_at: Option<String>,
}

/// Hand-written so the raw token cannot reach a log line, a panic message, or
/// an `{err:?}` rendering through this type.
impl fmt::Debug for Minted {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Minted")
            .field("token", &"(redacted)")
            .field("token_id", &self.token_id)
            .field("expires_at", &self.expires_at)
            .finish()
    }
}

/// A typed control-plane failure. `code` is what the flow branches on (§6.4's
/// `org_ambiguous`), `orgs` carries that code's extra payload.
#[derive(Debug)]
pub struct CpError {
    pub status: u16,
    pub code: Option<String>,
    pub message: String,
    /// `org_slug` values from an `org_ambiguous` body, in the order returned.
    pub orgs: Vec<String>,
}

impl fmt::Display for CpError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.code {
            Some(code) => write!(f, "[{code}] {} (HTTP {})", self.message, self.status),
            None => write!(f, "{} (HTTP {})", self.message, self.status),
        }
    }
}

impl std::error::Error for CpError {}

/// A client for one control plane, authenticated as one identity.
pub struct ControlPlane {
    http: reqwest::Client,
    base_url: String,
    /// An ID token, or a `dev:<external-id>` bearer (§6.3). Never a PAT.
    bearer: String,
}

impl ControlPlane {
    /// `base_url` may carry a trailing slash; paths are appended after it is
    /// trimmed, so `https://cp.example/` and `https://cp.example` behave the
    /// same.
    pub fn new(http: reqwest::Client, base_url: &str, bearer: String) -> ControlPlane {
        ControlPlane {
            http,
            base_url: base_url.trim_end_matches('/').to_string(),
            bearer,
        }
    }

    /// `GET /v1/me/workspaces` (§6.1 step 4).
    pub async fn memberships(&self) -> Result<Vec<Membership>> {
        let body = self
            .request(reqwest::Method::GET, "/v1/me/workspaces", None)
            .await?;
        let parsed: MembershipsBody = serde_json::from_value(body)
            .context("the control plane's workspace list was not the expected shape")?;
        Ok(parsed.workspaces)
    }

    /// `POST /v1/me/tokens` (§6.1 step 5).
    ///
    /// The scope is the point of the whole flow: `{"workspaces": [one],
    /// "max_role": role}` is intersected with live memberships on every
    /// resolve, so a leaked CLI credential reaches one workspace at one role.
    pub async fn mint(
        &self,
        name: &str,
        workspace: &str,
        max_role: &str,
        expires_at: &str,
    ) -> Result<Minted> {
        let body = json!({
            "name": name,
            "scope": { "workspaces": [workspace], "max_role": max_role },
            "expires_at": expires_at,
        });
        let response = self
            .request(reqwest::Method::POST, "/v1/me/tokens", Some(body))
            .await?;
        let parsed: MintBody = serde_json::from_value(response)
            .context("the control plane's mint response was not the expected shape")?;
        Ok(Minted {
            token: parsed.token,
            token_id: parsed.pat.token_id,
            expires_at: parsed.pat.expires_at,
        })
    }

    /// `DELETE /v1/me/tokens/{token_id}` — the saga's rollback and
    /// `logout --revoke` (§6.4, §6.5).
    pub async fn revoke(&self, token_id: &str) -> Result<()> {
        let path = format!(
            "/v1/me/tokens/{}",
            crate::client::encode_component(token_id)
        );
        self.request(reqwest::Method::DELETE, &path, None).await?;
        Ok(())
    }

    /// One request, with the typed-error mapping every caller shares.
    async fn request(
        &self,
        method: reqwest::Method,
        path: &str,
        body: Option<Value>,
    ) -> Result<Value> {
        let url = format!("{}{path}", self.base_url);
        let mut request = self.http.request(method, &url).bearer_auth(&self.bearer);
        if let Some(body) = body {
            request = request.json(&body);
        }
        let response = request
            .send()
            .await
            .with_context(|| format!("cannot reach the control plane at {url}"))?;

        let status = response.status();
        let text = read_capped(response, &url).await?;

        if !status.is_success() {
            return Err(parse_error(status.as_u16(), &text).into());
        }
        // `DELETE` answers 204 with no body, and a mint/list always has one.
        if text.trim().is_empty() {
            return Ok(Value::Null);
        }
        serde_json::from_str(&text)
            .with_context(|| format!("{url} returned a body that is not JSON"))
    }
}

#[derive(Deserialize)]
struct MembershipsBody {
    #[serde(default)]
    workspaces: Vec<Membership>,
}

#[derive(Deserialize)]
struct MintBody {
    token: String,
    pat: PatView,
}

#[derive(Deserialize)]
struct PatView {
    token_id: String,
    #[serde(default)]
    expires_at: Option<String>,
}

/// Read a response body, refusing to buffer more than [`MAX_BODY_BYTES`].
/// Chunk-by-chunk so the cap holds for a chunked response with no declared
/// length.
async fn read_capped(mut response: reqwest::Response, url: &str) -> Result<String> {
    let mut buf: Vec<u8> = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .with_context(|| format!("read the response from {url}"))?
    {
        if buf.len().saturating_add(chunk.len()) > MAX_BODY_BYTES {
            anyhow::bail!(
                "{url} returned more than {MAX_BODY_BYTES} bytes — refusing to buffer it"
            );
        }
        buf.extend_from_slice(&chunk);
    }
    Ok(String::from_utf8_lossy(&buf).to_string())
}

/// Map a non-success body to [`CpError`], reading skardi-global's nested
/// envelope and falling back to the raw first line for anything else (a proxy
/// error page, an HTML 502).
fn parse_error(status: u16, text: &str) -> CpError {
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
        #[serde(default)]
        orgs: Vec<OrgRef>,
    }
    #[derive(Deserialize)]
    struct OrgRef {
        #[serde(default)]
        org_slug: Option<String>,
    }

    match serde_json::from_str::<Envelope>(text) {
        Ok(envelope) => CpError {
            status,
            code: envelope.error.code,
            message: envelope
                .error
                .message
                .unwrap_or_else(|| "no message".to_string()),
            orgs: envelope
                .error
                .orgs
                .into_iter()
                .filter_map(|o| o.org_slug)
                .collect(),
        },
        Err(_) => CpError {
            status,
            code: None,
            message: text.lines().next().unwrap_or("").to_string(),
            orgs: Vec::new(),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::{Membership, MembershipsBody, Minted, parse_error};

    /// The raw token must not reach a log line, a panic message, or an
    /// `{err:?}` rendering through this type — so `Debug` is hand-written, and
    /// that is worth pinning rather than trusting.
    #[test]
    fn a_minted_tokens_debug_output_redacts_the_secret() {
        let minted = Minted {
            token: "skardi_pat_the_real_secret".to_string(),
            token_id: "tok-1".to_string(),
            expires_at: Some("2026-11-22T12:00:00Z".to_string()),
        };
        let rendered = format!("{minted:?}");
        assert!(!rendered.contains("the_real_secret"), "{rendered}");
        assert!(rendered.contains("(redacted)"), "{rendered}");
        // The id and expiry are what a caller legitimately needs to see.
        assert!(rendered.contains("tok-1"), "{rendered}");
        assert!(rendered.contains("2026-11-22"), "{rendered}");
    }

    #[test]
    fn a_membership_names_its_context_and_knows_when_it_is_usable() {
        let active: Membership = serde_json::from_value(serde_json::json!({
            "org_slug": "acme",
            "tenant_slug": "acme-prod",
            "role": "admin",
            "provisioning_state": "active",
        }))
        .unwrap();
        assert_eq!(active.context_name(), "acme/acme-prod");
        assert!(active.is_active());
        // `display_name` and `gateway_url` are absent on today's wire (§7.1 is
        // M4), which must deserialize rather than fail.
        assert_eq!(active.display_name, None);
        assert_eq!(active.gateway_url, None);

        let provisioning: Membership = serde_json::from_value(serde_json::json!({
            "org_slug": "acme",
            "tenant_slug": "acme-new",
            "role": "admin",
            "provisioning_state": "provisioning",
        }))
        .unwrap();
        assert!(!provisioning.is_active());
    }

    /// The literal shape skardi-cloud's `/v1/me/workspaces` emits: five
    /// `MembershipView` fields flattened alongside an optional `gateway_url`,
    /// with the key OMITTED (not null, not empty) when the deployment has no
    /// gateway URL configured.
    ///
    /// Pinned against the control plane's own projection rather than against
    /// prose, so a change to that wire shape fails here instead of at a user's
    /// first `login`.
    #[test]
    fn the_control_planes_membership_projection_deserializes_verbatim() {
        let body: super::MembershipsBody = serde_json::from_str(
            r#"{"workspaces": [
                {"org_slug": "acme", "tenant_slug": "acme-prod",
                 "display_name": "Prod", "role": "member",
                 "provisioning_state": "active",
                 "gateway_url": "https://gateway-test.skardi.ai"},
                {"org_slug": "acme", "tenant_slug": "acme-staging",
                 "display_name": "Staging", "role": "member",
                 "provisioning_state": "active"}
            ]}"#,
        )
        .unwrap();

        assert_eq!(body.workspaces.len(), 2);
        assert_eq!(
            body.workspaces[0].gateway_url.as_deref(),
            Some("https://gateway-test.skardi.ai")
        );
        assert_eq!(body.workspaces[0].display_name.as_deref(), Some("Prod"));
        assert_eq!(body.workspaces[0].context_name(), "acme/acme-prod");
        // Omitted, not null: §6.2's precedence takes over from here.
        assert_eq!(body.workspaces[1].gateway_url, None);
    }

    /// The nested envelope, and the fallbacks for a body that is not one — a
    /// proxy's HTML error page, or an empty 502.
    #[test]
    fn error_bodies_map_to_a_typed_failure_or_their_first_line() {
        let typed = parse_error(
            400,
            r#"{"error": {"code": "org_ambiguous", "message": "several orgs",
                "orgs": [{"org_slug": "acme"}, {"display_name": "no slug here"}]}}"#,
        );
        assert_eq!(typed.code.as_deref(), Some("org_ambiguous"));
        assert_eq!(typed.message, "several orgs");
        // An entry with no `org_slug` is dropped rather than rendered as an
        // empty name.
        assert_eq!(typed.orgs, ["acme"]);
        assert_eq!(typed.to_string(), "[org_ambiguous] several orgs (HTTP 400)");

        let html = parse_error(502, "<html><body>Bad Gateway</body></html>\nmore");
        assert_eq!(html.code, None);
        assert_eq!(html.message, "<html><body>Bad Gateway</body></html>");
        assert_eq!(
            html.to_string(),
            "<html><body>Bad Gateway</body></html> (HTTP 502)"
        );

        // A well-formed envelope with no message still says something.
        let bare = parse_error(500, r#"{"error": {"code": "internal"}}"#);
        assert_eq!(bare.message, "no message");
    }
}
