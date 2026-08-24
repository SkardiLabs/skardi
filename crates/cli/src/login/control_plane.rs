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

/// Cap on a control-plane response body. Memberships and one PAT are a few
/// hundred bytes; this is the "something is very wrong" bound, and small
/// enough that a runaway endpoint cannot exhaust memory.
const MAX_BODY_BYTES: usize = 1024 * 1024;

/// One membership as `/v1/me/workspaces` returns it (`MembershipView`).
///
/// `gateway_url` is `Option` because it is M4's addition (§7.1): a control
/// plane that predates it simply omits the key, and §6.2's precedence then
/// decides — flag, env, or a typed error naming `--server`.
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
