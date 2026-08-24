//! The single-use loopback redirect target (§6.1 step 2, §9.1).
//!
//! Bound to `127.0.0.1:0` — never `0.0.0.0`, and never a fixed port: the
//! kernel picks a free one, so two logins can run at once and nothing on the
//! network can reach the listener. It speaks just enough HTTP to read one
//! request line and answer one page, because pulling a server framework into
//! the CLI to serve a single GET would be the larger cost.
//!
//! "Single-use" is about the CALLBACK, not the connection: a browser also asks
//! for `/favicon.ico`, may send a preflight, and may probe the port before
//! following the redirect. Those get a 404 and the listener keeps waiting, up
//! to [`MAX_UNRELATED_REQUESTS`], so a favicon cannot consume the one
//! authorization response. The first request that IS a callback ends the wait.

use anyhow::{Context, Result, bail};
use percent_encoding::percent_decode_str;
use std::collections::HashMap;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// The path the authorization server is told to redirect to.
pub const CALLBACK_PATH: &str = "/callback";

/// Cap on request-line bytes read from one connection. A callback URL carries
/// a code and a state; anything approaching this is not one.
const MAX_REQUEST_BYTES: usize = 8 * 1024;

/// How many non-callback requests are answered and ignored before giving up.
/// Bounded so a chatty client cannot keep the wait alive indefinitely — the
/// timeout is the other bound.
const MAX_UNRELATED_REQUESTS: usize = 8;

/// A bound loopback listener waiting for the authorization redirect.
pub struct Loopback {
    listener: TcpListener,
    port: u16,
}

/// What the authorization server sent back.
///
/// `Debug` carries the code: it is single-use, already redeemed or refused by
/// the time anything would print this, and useless without the PKCE verifier
/// that never leaves memory. It exists so a failed `unwrap` in a test says
/// what arrived.
#[derive(Debug)]
pub struct Callback {
    pub code: Option<String>,
    pub state: Option<String>,
    /// OAuth 2.0 `error` (e.g. `access_denied`) when the user refused or the
    /// provider rejected the request.
    pub error: Option<String>,
    pub error_description: Option<String>,
}

impl Loopback {
    pub async fn bind() -> Result<Loopback> {
        let listener = TcpListener::bind(("127.0.0.1", 0))
            .await
            .context("bind a loopback listener for the login redirect")?;
        let port = listener
            .local_addr()
            .context("read the loopback listener's port")?
            .port();
        Ok(Loopback { listener, port })
    }

    /// The `redirect_uri` to register with the authorization request. Literal
    /// `127.0.0.1` rather than `localhost`: the name can resolve to `::1`
    /// first, which is a different listener than the one bound above.
    pub fn redirect_uri(&self) -> String {
        format!("http://127.0.0.1:{}{CALLBACK_PATH}", self.port)
    }

    /// Wait for the redirect, or fail once `timeout` elapses.
    ///
    /// Consumes `self`, so the listener — and the port — is released when this
    /// returns, on the timeout path as much as on the success path.
    pub async fn wait(self, timeout: Duration) -> Result<Callback> {
        match tokio::time::timeout(timeout, self.accept_callback()).await {
            Ok(result) => result,
            Err(_) => bail!(
                "no authorization response within {}s — the login was not completed",
                timeout.as_secs()
            ),
        }
    }

    async fn accept_callback(&self) -> Result<Callback> {
        for _ in 0..=MAX_UNRELATED_REQUESTS {
            let (mut stream, _peer) = self
                .listener
                .accept()
                .await
                .context("accept the login redirect")?;

            let Some(target) = read_request_target(&mut stream).await? else {
                respond(&mut stream, "400 Bad Request", "Malformed request.").await;
                continue;
            };
            let (path, query) = match target.split_once('?') {
                Some((path, query)) => (path, query),
                None => (target.as_str(), ""),
            };
            if path != CALLBACK_PATH {
                respond(&mut stream, "404 Not Found", "Not the login callback.").await;
                continue;
            }

            let params = parse_query(query);
            let callback = Callback {
                code: params.get("code").cloned(),
                state: params.get("state").cloned(),
                error: params.get("error").cloned(),
                error_description: params.get("error_description").cloned(),
            };
            // A bare `/callback` with neither is a probe, not a response.
            if callback.code.is_none() && callback.error.is_none() {
                respond(&mut stream, "400 Bad Request", "No authorization response.").await;
                continue;
            }

            // The page never echoes a query value: it is rendered in a browser
            // that just followed a redirect built by someone else, and nothing
            // in the response needs the caller's input to be useful.
            let body = match callback.error {
                None => "Signed in. You can close this tab and return to the terminal.",
                Some(_) => "Sign-in failed. Return to the terminal for the details.",
            };
            respond(&mut stream, "200 OK", body).await;
            return Ok(callback);
        }
        bail!(
            "the login redirect never arrived after {MAX_UNRELATED_REQUESTS} unrelated requests on the callback port"
        )
    }
}

/// Read the request target out of the first line, bounded. `None` when the
/// line is not a parseable `GET <target> HTTP/1.x`.
async fn read_request_target(stream: &mut TcpStream) -> Result<Option<String>> {
    let mut buf = Vec::new();
    let mut byte = [0u8; 1];
    while buf.len() < MAX_REQUEST_BYTES {
        match stream.read(&mut byte).await {
            Ok(0) => break,
            Ok(_) => {
                if byte[0] == b'\n' {
                    break;
                }
                buf.push(byte[0]);
            }
            // A client that hangs up mid-line is noise, not a failure of the
            // wait: report it as an unparseable line and keep listening.
            Err(_) => return Ok(None),
        }
    }
    let line = String::from_utf8_lossy(&buf).trim().to_string();
    let mut parts = line.split_whitespace();
    match (parts.next(), parts.next()) {
        (Some("GET"), Some(target)) => Ok(Some(target.to_string())),
        _ => Ok(None),
    }
}

/// Percent-decode a `application/x-www-form-urlencoded` query into pairs.
/// Later duplicates lose to earlier ones, so a second `code=` appended to the
/// URL cannot displace the real one.
fn parse_query(query: &str) -> HashMap<String, String> {
    let mut params = HashMap::new();
    for pair in query.split('&').filter(|p| !p.is_empty()) {
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        let decode = |raw: &str| {
            percent_decode_str(&raw.replace('+', " "))
                .decode_utf8_lossy()
                .to_string()
        };
        params.entry(decode(key)).or_insert_with(|| decode(value));
    }
    params
}

/// Write one minimal HTML response. Best-effort: the browser tab's contents
/// are cosmetic, and a write failure here must not lose an authorization code
/// that was already read off the wire.
async fn respond(stream: &mut TcpStream, status: &str, body: &str) {
    let page = format!(
        "<!doctype html><html><head><meta charset=\"utf-8\"><title>skardi login</title></head>\
         <body style=\"font-family:system-ui;margin:3rem\"><p>{body}</p></body></html>"
    );
    let response = format!(
        "HTTP/1.1 {status}\r\nContent-Type: text/html; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{page}",
        page.len()
    );
    let _ = stream.write_all(response.as_bytes()).await;
    let _ = stream.flush().await;
}

#[cfg(test)]
mod tests {
    use super::{Loopback, parse_query};
    use std::time::Duration;

    fn port_of(redirect_uri: &str) -> u16 {
        redirect_uri
            .trim_start_matches("http://127.0.0.1:")
            .trim_end_matches("/callback")
            .parse()
            .unwrap()
    }

    #[test]
    fn query_parsing_decodes_and_keeps_the_first_of_a_duplicate() {
        let params = parse_query("code=a%2Fb&state=x+y&code=second&empty=");
        assert_eq!(params["code"], "a/b", "percent-decoded");
        assert_eq!(params["state"], "x y", "+ is a space in a query");
        assert_eq!(params["empty"], "");
        assert_eq!(
            params["code"], "a/b",
            "an appended duplicate must not displace the real code"
        );
    }

    #[tokio::test]
    async fn bind_is_loopback_only_and_the_uri_names_the_bound_port() {
        let loopback = Loopback::bind().await.unwrap();
        let uri = loopback.redirect_uri();
        assert!(uri.starts_with("http://127.0.0.1:"), "{uri}");
        assert!(uri.ends_with("/callback"), "{uri}");
        assert!(port_of(&uri) > 0);
    }

    /// §6.1 step 2: "120-second timeout, then fail cleanly and release the
    /// port." Rebinding the same port is the check that it was released.
    #[tokio::test]
    async fn a_timeout_fails_cleanly_and_releases_the_port() {
        let loopback = Loopback::bind().await.unwrap();
        let port = port_of(&loopback.redirect_uri());

        let err = loopback
            .wait(Duration::from_millis(50))
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("no authorization response within"), "{err}");

        tokio::net::TcpListener::bind(("127.0.0.1", port))
            .await
            .expect("the port must be free after the wait returns");
    }

    /// A browser asks for `/favicon.ico`, and something may probe the port
    /// before the redirect lands. Neither may consume the single callback.
    #[tokio::test]
    async fn unrelated_requests_do_not_consume_the_callback() {
        let loopback = Loopback::bind().await.unwrap();
        let uri = loopback.redirect_uri();
        let base = uri.trim_end_matches("/callback").to_string();

        let waiter = tokio::spawn(async move { loopback.wait(Duration::from_secs(5)).await });
        let http = reqwest::Client::new();
        // Noise first, each fully awaited so the ordering is deterministic.
        let _ = http.get(format!("{base}/favicon.ico")).send().await;
        let _ = http.get(format!("{base}/callback")).send().await;
        let response = http
            .get(format!("{uri}?code=the-code&state=the-state"))
            .send()
            .await
            .unwrap();
        assert!(response.status().is_success());
        assert!(
            response.text().await.unwrap().contains("Signed in"),
            "the browser tab should say the sign-in completed"
        );

        let callback = waiter.await.unwrap().unwrap();
        assert_eq!(callback.code.as_deref(), Some("the-code"));
        assert_eq!(callback.state.as_deref(), Some("the-state"));
    }

    /// The noise a real port sees: a POST, a request line that is not HTTP at
    /// all, and a connection that opens and closes. None may consume the
    /// single callback, and each is answered rather than left hanging.
    #[tokio::test]
    async fn malformed_and_non_get_requests_are_answered_and_ignored() {
        use tokio::io::AsyncWriteExt as _;

        let loopback = Loopback::bind().await.unwrap();
        let uri = loopback.redirect_uri();
        let addr = uri
            .trim_start_matches("http://")
            .trim_end_matches("/callback")
            .to_string();

        let waiter = tokio::spawn(async move { loopback.wait(Duration::from_secs(5)).await });

        // A POST to the right path: not a redirect, so not the callback.
        let _ = reqwest::Client::new()
            .post(&uri)
            .body("code=nope")
            .send()
            .await;
        // A request line that is not a request line.
        let mut raw = tokio::net::TcpStream::connect(&addr).await.unwrap();
        raw.write_all(b"GARBAGE\r\n\r\n").await.unwrap();
        drop(raw);
        // A connection that opens and hangs up without sending anything.
        drop(tokio::net::TcpStream::connect(&addr).await.unwrap());

        let callback = reqwest::get(format!("{uri}?code=real&state=s"))
            .await
            .unwrap();
        assert!(callback.status().is_success());
        let received = waiter.await.unwrap().unwrap();
        assert_eq!(received.code.as_deref(), Some("real"));
    }

    /// Enough noise to exhaust the allowance ends the wait with a message
    /// naming why, instead of holding the port until the timeout.
    #[tokio::test]
    async fn too_much_noise_gives_up_rather_than_waiting_out_the_timeout() {
        let loopback = Loopback::bind().await.unwrap();
        let base = loopback
            .redirect_uri()
            .trim_end_matches("/callback")
            .to_string();

        let waiter = tokio::spawn(async move { loopback.wait(Duration::from_secs(30)).await });
        let http = reqwest::Client::new();
        for _ in 0..=super::MAX_UNRELATED_REQUESTS {
            let _ = http.get(format!("{base}/favicon.ico")).send().await;
        }

        let err = waiter.await.unwrap().unwrap_err().to_string();
        assert!(err.contains("unrelated requests"), "{err}");
    }

    #[tokio::test]
    async fn a_provider_error_is_returned_rather_than_waited_out() {
        let loopback = Loopback::bind().await.unwrap();
        let uri = loopback.redirect_uri();

        let waiter = tokio::spawn(async move { loopback.wait(Duration::from_secs(5)).await });
        let response = reqwest::get(format!(
            "{uri}?error=access_denied&error_description=user%20refused"
        ))
        .await
        .unwrap();
        assert!(response.text().await.unwrap().contains("Sign-in failed"));

        let callback = waiter.await.unwrap().unwrap();
        assert_eq!(callback.error.as_deref(), Some("access_denied"));
        assert_eq!(callback.error_description.as_deref(), Some("user refused"));
    }
}
