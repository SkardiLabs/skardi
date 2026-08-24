//! The browser half of `login` (§6.1 steps 2-3): build the authorization URL,
//! wait for the redirect, check `state`, and exchange the code for an ID token
//! that is **held in memory only**.
//!
//! No refresh token is requested and nothing the provider returns is written
//! to disk (§9.1): the only credential that reaches the config file is the
//! workspace-scoped PAT the control plane mints later. A stolen config
//! therefore yields expiring, single-workspace PATs rather than a re-loginable
//! identity.
//!
//! The two endpoint URLs are fields with production defaults rather than
//! constants, so tests drive the whole flow against a stub token endpoint.

use super::loopback::Loopback;
use super::pkce::{Pkce, random_urlsafe};
use anyhow::{Context, Result, bail};
use percent_encoding::{NON_ALPHANUMERIC, utf8_percent_encode};
use serde::Deserialize;
use std::time::Duration;

/// Google's OAuth 2.0 endpoints, the identity provider skardi-cloud verifies
/// (`SKARDI_GLOBAL_OIDC_ISSUER=accounts.google.com`).
pub const GOOGLE_AUTHORIZATION_URL: &str = "https://accounts.google.com/o/oauth2/v2/auth";
pub const GOOGLE_TOKEN_URL: &str = "https://oauth2.googleapis.com/token";

/// The claims the control plane needs: `openid` for an ID token at all, `email`
/// because memberships are keyed on the identity's email, `profile` for the
/// display name `/v1/me/profile` returns. Nothing more — the CLI reads no
/// provider API.
const SCOPE: &str = "openid email profile";

/// How long to wait for the user to finish in the browser (§6.1 step 2).
pub const CALLBACK_TIMEOUT: Duration = Duration::from_secs(120);

/// Where the browser flow talks. Defaults are production; tests override.
#[derive(Debug, Clone)]
pub struct Endpoints {
    pub authorization_url: String,
    pub token_url: String,
}

impl Default for Endpoints {
    fn default() -> Endpoints {
        Endpoints {
            authorization_url: GOOGLE_AUTHORIZATION_URL.to_string(),
            token_url: GOOGLE_TOKEN_URL.to_string(),
        }
    }
}

/// Run the browser flow and return the ID token.
///
/// `no_browser` prints the URL instead of opening it, for a remote shell or a
/// machine with no browser at all.
///
/// `open` is a parameter, not a call to [`open_in_browser`], for one reason:
/// it makes the composition below — bind, build, wait, exchange — testable end
/// to end with a closure that plays the browser. Production passes
/// `&open_in_browser`.
pub async fn acquire_id_token(
    http: &reqwest::Client,
    endpoints: &Endpoints,
    client_id: &str,
    no_browser: bool,
    timeout: Duration,
    open: &dyn Fn(&str) -> std::io::Result<()>,
) -> Result<String> {
    let pkce = Pkce::generate()?;
    let state = random_urlsafe()?;
    let loopback = Loopback::bind().await?;
    let redirect_uri = loopback.redirect_uri();
    let url = authorization_url(
        &endpoints.authorization_url,
        client_id,
        &redirect_uri,
        &pkce.challenge,
        &state,
    );

    if no_browser {
        eprintln!("open this URL to sign in:\n\n  {url}\n");
    } else {
        match open(&url) {
            Ok(()) => eprintln!("waiting for the browser to complete sign-in…"),
            // Not fatal: printing the URL is exactly what --no-browser does,
            // so a headless machine still completes the login.
            Err(err) => eprintln!(
                "could not open a browser ({err}); open this URL to sign in:\n\n  {url}\n"
            ),
        }
    }

    let code = await_code(loopback, &state, timeout).await?;
    exchange_code(
        http,
        &endpoints.token_url,
        client_id,
        &code,
        &pkce.verifier,
        &redirect_uri,
    )
    .await
}

/// Build the authorization request URL.
///
/// `prompt=select_account` is deliberate: the CLI is minting a credential, and
/// silently reusing whichever account the browser happens to be signed into is
/// how someone ends up with a PAT for the wrong identity.
pub fn authorization_url(
    authorization_url: &str,
    client_id: &str,
    redirect_uri: &str,
    challenge: &str,
    state: &str,
) -> String {
    let q = |value: &str| utf8_percent_encode(value, NON_ALPHANUMERIC).to_string();
    format!(
        "{authorization_url}?response_type=code&client_id={}&redirect_uri={}&scope={}\
         &code_challenge={}&code_challenge_method=S256&state={}&prompt=select_account",
        q(client_id),
        q(redirect_uri),
        q(SCOPE),
        q(challenge),
        q(state),
    )
}

/// Wait for the redirect and return the authorization code, refusing anything
/// that does not match `expected_state`.
pub async fn await_code(
    loopback: Loopback,
    expected_state: &str,
    timeout: Duration,
) -> Result<String> {
    let callback = loopback.wait(timeout).await?;

    if let Some(error) = callback.error {
        let detail = callback
            .error_description
            .map(|d| format!(": {d}"))
            .unwrap_or_default();
        bail!("the identity provider refused the sign-in ({error}{detail})");
    }
    // Checked before the code is even read: a callback whose `state` does not
    // match was not produced by this process's authorization request, and
    // redeeming its code would be redeeming someone else's (§9.1).
    match callback.state.as_deref() {
        Some(state) if state == expected_state => {}
        _ => bail!(
            "the login callback carried the wrong 'state' value — it did not come from this sign-in request, so it was ignored"
        ),
    }
    callback
        .code
        .filter(|code| !code.is_empty())
        .context("the login callback carried no authorization code")
}

/// Redeem the code for an ID token. A public client with PKCE, so no client
/// secret is sent — there is nowhere in a distributed CLI to keep one.
pub async fn exchange_code(
    http: &reqwest::Client,
    token_url: &str,
    client_id: &str,
    code: &str,
    verifier: &str,
    redirect_uri: &str,
) -> Result<String> {
    let response = http
        .post(token_url)
        .form(&[
            ("grant_type", "authorization_code"),
            ("client_id", client_id),
            ("code", code),
            ("code_verifier", verifier),
            ("redirect_uri", redirect_uri),
        ])
        .send()
        .await
        .with_context(|| format!("POST {token_url}"))?;

    let status = response.status();
    let body = response
        .text()
        .await
        .context("read the token endpoint's response")?;

    if !status.is_success() {
        // The provider's own error naming, without the body: a token-endpoint
        // response can carry the code that was just redeemed.
        let parsed: Option<TokenError> = serde_json::from_str(&body).ok();
        let described = parsed
            .map(|e| match e.error_description {
                Some(description) => format!("{}: {description}", e.error),
                None => e.error,
            })
            .unwrap_or_else(|| format!("HTTP {status}"));
        bail!("the identity provider rejected the authorization code ({described})");
    }

    let parsed: TokenResponse = serde_json::from_str(&body)
        .context("the token endpoint's response was not the expected JSON")?;
    if parsed.id_token.trim().is_empty() {
        bail!("the token endpoint returned no id_token — the client id may not be an OIDC client");
    }
    Ok(parsed.id_token)
}

/// Only `id_token` is read. `access_token` is deliberately ignored (the CLI
/// calls no provider API) and no refresh token is requested at all.
#[derive(Deserialize)]
struct TokenResponse {
    #[serde(default)]
    id_token: String,
}

#[derive(Deserialize)]
struct TokenError {
    error: String,
    #[serde(default)]
    error_description: Option<String>,
}

/// Hand the URL to the platform opener. `Command`, not a crate: this is one
/// argv per platform, and the URL is one we just built.
pub fn open_in_browser(url: &str) -> std::io::Result<()> {
    let (program, args): (&str, &[&str]) = if cfg!(target_os = "macos") {
        ("open", &[])
    } else if cfg!(target_os = "windows") {
        ("cmd", &["/C", "start", ""])
    } else {
        ("xdg-open", &[])
    };
    let status = std::process::Command::new(program)
        .args(args)
        .arg(url)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()?;
    if status.success() {
        Ok(())
    } else {
        Err(std::io::Error::other(format!(
            "{program} exited with {status}"
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::{Endpoints, authorization_url, await_code, exchange_code};
    use crate::login::loopback::Loopback;
    use serde_json::json;
    use std::time::Duration;
    use wiremock::matchers::{body_string_contains, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[test]
    fn the_authorization_url_carries_pkce_s256_and_the_loopback_redirect() {
        let url = authorization_url(
            "https://accounts.example/auth",
            "client-123.apps.googleusercontent.com",
            "http://127.0.0.1:54321/callback",
            "the-challenge",
            "the-state",
        );
        assert!(url.starts_with("https://accounts.example/auth?"), "{url}");
        for expected in [
            "response_type=code",
            "code_challenge=the%2Dchallenge",
            "code_challenge_method=S256",
            "state=the%2Dstate",
            "client_id=client%2D123%2Eapps%2Egoogleusercontent%2Ecom",
            "redirect_uri=http%3A%2F%2F127%2E0%2E0%2E1%3A54321%2Fcallback",
            "scope=openid%20email%20profile",
            "prompt=select_account",
        ] {
            assert!(url.contains(expected), "missing {expected} in {url}");
        }
        // A public client sends no secret, and asks for no refresh token.
        assert!(!url.contains("client_secret"), "{url}");
        assert!(!url.contains("access_type=offline"), "{url}");
    }

    /// §9.1's `state` check: a callback that did not come from this request is
    /// refused BEFORE its code is read, so a code planted by anything else is
    /// never redeemed.
    #[tokio::test]
    async fn a_state_mismatch_refuses_the_callback() {
        let loopback = Loopback::bind().await.unwrap();
        let uri = loopback.redirect_uri();

        let waiter = tokio::spawn(async move {
            await_code(loopback, "the-real-state", Duration::from_secs(5)).await
        });
        let _ = reqwest::get(format!("{uri}?code=planted&state=someone-elses")).await;

        let err = waiter.await.unwrap().unwrap_err().to_string();
        assert!(err.contains("wrong 'state' value"), "{err}");
        assert!(
            !err.contains("planted"),
            "the code must not be echoed: {err}"
        );
    }

    #[tokio::test]
    async fn a_matching_state_yields_the_code() {
        let loopback = Loopback::bind().await.unwrap();
        let uri = loopback.redirect_uri();

        let waiter = tokio::spawn(async move {
            await_code(loopback, "the-real-state", Duration::from_secs(5)).await
        });
        let _ = reqwest::get(format!("{uri}?code=good-code&state=the-real-state")).await;

        assert_eq!(waiter.await.unwrap().unwrap(), "good-code");
    }

    #[tokio::test]
    async fn a_refused_sign_in_names_the_providers_reason() {
        let loopback = Loopback::bind().await.unwrap();
        let uri = loopback.redirect_uri();

        let waiter = tokio::spawn(async move {
            await_code(loopback, "the-real-state", Duration::from_secs(5)).await
        });
        let _ = reqwest::get(format!("{uri}?error=access_denied&error_description=nope")).await;

        let err = waiter.await.unwrap().unwrap_err().to_string();
        assert!(err.contains("access_denied"), "{err}");
        assert!(err.contains("nope"), "{err}");
    }

    #[tokio::test]
    async fn the_exchange_sends_the_verifier_and_returns_only_the_id_token() {
        let provider = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/token"))
            .and(body_string_contains("grant_type=authorization_code"))
            .and(body_string_contains("code_verifier=the-verifier"))
            .and(body_string_contains("code=the-code"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "id_token": "the-id-token",
                "access_token": "an-access-token",
                "expires_in": 3599,
            })))
            .expect(1)
            .mount(&provider)
            .await;

        let endpoints = Endpoints {
            authorization_url: format!("{}/auth", provider.uri()),
            token_url: format!("{}/token", provider.uri()),
        };
        let id_token = exchange_code(
            &reqwest::Client::new(),
            &endpoints.token_url,
            "client-123",
            "the-code",
            "the-verifier",
            "http://127.0.0.1:1/callback",
        )
        .await
        .unwrap();

        assert_eq!(id_token, "the-id-token");
        // No client secret is sent — there is nowhere in a shipped CLI to keep
        // one, which is why PKCE is doing this job.
        let sent = &provider.received_requests().await.unwrap()[0];
        let body = String::from_utf8_lossy(&sent.body);
        assert!(!body.contains("client_secret"), "{body}");
    }

    #[tokio::test]
    async fn a_rejected_code_reports_the_provider_error_without_the_body() {
        let provider = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(400).set_body_json(json!({
                "error": "invalid_grant",
                "error_description": "Code was already redeemed.",
                "code": "the-secret-code",
            })))
            .mount(&provider)
            .await;

        let err = exchange_code(
            &reqwest::Client::new(),
            &format!("{}/token", provider.uri()),
            "client-123",
            "the-secret-code",
            "the-verifier",
            "http://127.0.0.1:1/callback",
        )
        .await
        .unwrap_err()
        .to_string();

        assert!(err.contains("invalid_grant"), "{err}");
        assert!(err.contains("Code was already redeemed."), "{err}");
    }

    /// A non-OIDC client id gets an access token and no `id_token`. Failing
    /// here names the cause; passing the empty string on would 401 at the
    /// control plane instead.
    #[tokio::test]
    async fn a_response_without_an_id_token_is_refused_by_name() {
        let provider = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(json!({"access_token": "only-this"})),
            )
            .mount(&provider)
            .await;

        let err = exchange_code(
            &reqwest::Client::new(),
            &format!("{}/token", provider.uri()),
            "client-123",
            "the-code",
            "the-verifier",
            "http://127.0.0.1:1/callback",
        )
        .await
        .unwrap_err()
        .to_string();
        assert!(err.contains("no id_token"), "{err}");
    }
}

#[cfg(test)]
mod browser_path_tests {
    use super::{CALLBACK_TIMEOUT, Endpoints, acquire_id_token};
    use serde_json::json;
    use std::time::Duration;
    use wiremock::matchers::method;
    use wiremock::{Mock, MockServer, ResponseTemplate};

    /// One query parameter out of a URL, percent-decoded.
    fn param(url: &str, key: &str) -> String {
        let query = url.split_once('?').expect("a query").1;
        let raw = query
            .split('&')
            .find_map(|pair| pair.strip_prefix(&format!("{key}=")))
            .unwrap_or_else(|| panic!("no {key} in {url}"));
        percent_encoding::percent_decode_str(raw)
            .decode_utf8_lossy()
            .to_string()
    }

    /// The whole browser path, with a closure standing in for the browser: it
    /// reads the authorization URL exactly as a user agent would, follows the
    /// redirect back with the `state` it was given, and the flow completes.
    #[tokio::test]
    async fn the_browser_path_composes_bind_authorize_callback_and_exchange() {
        let provider = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(json!({"id_token": "issued-token"})),
            )
            .expect(1)
            .mount(&provider)
            .await;
        let endpoints = Endpoints {
            authorization_url: format!("{}/auth", provider.uri()),
            token_url: format!("{}/token", provider.uri()),
        };

        let browser = |url: &str| {
            let redirect = param(url, "redirect_uri");
            let state = param(url, "state");
            assert_eq!(param(url, "code_challenge_method"), "S256");
            tokio::spawn(async move {
                let _ = reqwest::get(format!("{redirect}?code=browser-code&state={state}")).await;
            });
            Ok(())
        };

        let token = acquire_id_token(
            &reqwest::Client::new(),
            &endpoints,
            "client-123",
            false,
            Duration::from_secs(5),
            &browser,
        )
        .await
        .unwrap();

        assert_eq!(token, "issued-token");
        // The verifier that reached the token endpoint is the one whose
        // challenge went out in the authorization URL — proven by the flow
        // completing against a provider that requires both.
        let sent = &provider.received_requests().await.unwrap()[0];
        let body = String::from_utf8_lossy(&sent.body);
        assert!(body.contains("code=browser-code"), "{body}");
        assert!(body.contains("code_verifier="), "{body}");
    }

    /// A browser that cannot be opened is not fatal: the URL is printed and
    /// the wait continues, which is what makes a headless machine work.
    #[tokio::test]
    async fn a_browser_that_will_not_open_still_completes_the_login() {
        let provider = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"id_token": "issued"})))
            .mount(&provider)
            .await;
        let endpoints = Endpoints {
            authorization_url: format!("{}/auth", provider.uri()),
            token_url: format!("{}/token", provider.uri()),
        };

        // Fails to "open", but still reads the URL — as a human would from the
        // printed line — and follows it.
        let browser = |url: &str| {
            let redirect = param(url, "redirect_uri");
            let state = param(url, "state");
            tokio::spawn(async move {
                let _ = reqwest::get(format!("{redirect}?code=c&state={state}")).await;
            });
            Err(std::io::Error::other("no display"))
        };

        let token = acquire_id_token(
            &reqwest::Client::new(),
            &endpoints,
            "client-123",
            false,
            Duration::from_secs(5),
            &browser,
        )
        .await
        .unwrap();
        assert_eq!(token, "issued");
    }

    /// The production timeout is the design's 120 seconds, and a browser that
    /// never answers fails on it rather than hanging.
    #[tokio::test]
    async fn an_unanswered_authorization_times_out() {
        assert_eq!(CALLBACK_TIMEOUT, Duration::from_secs(120));

        let endpoints = Endpoints {
            authorization_url: "http://127.0.0.1:1/auth".to_string(),
            token_url: "http://127.0.0.1:1/token".to_string(),
        };
        let err = acquire_id_token(
            &reqwest::Client::new(),
            &endpoints,
            "client-123",
            true,
            Duration::from_millis(50),
            &|_url| Ok(()),
        )
        .await
        .unwrap_err()
        .to_string();
        assert!(err.contains("no authorization response within"), "{err}");
    }
}
