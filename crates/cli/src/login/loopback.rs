//! The single-use loopback redirect target (§6.1 step 2, §9.1).
//!
//! Bound to `127.0.0.1:0` — never `0.0.0.0`, and never a fixed port: the
//! kernel picks a free one, so two logins can run at once and nothing on the
//! network can reach the listener. It speaks just enough HTTP to read one
//! request line and answer one page, because pulling a server framework into
//! the CLI to serve a single GET would be the larger cost.
//!
//! "Single-use" is about THIS sign-in's callback, not the connection: a browser
//! also asks for `/favicon.ico`, may send a preflight, and may probe the port
//! before following the redirect. Those get a 404 and the listener keeps
//! waiting, up to [`MAX_UNRELATED_REQUESTS`], so a favicon cannot consume the
//! one authorization response.
//!
//! `state` is what decides membership, and it is checked HERE rather than by
//! the caller (§9.1: "a `state` checked on callback", unqualified). Anything on
//! this machine can reach a loopback port, and a browser can be induced to
//! fetch one cross-site — so a request carrying `?error=access_denied` with no
//! `state`, or a planted `?code=…&state=guess`, must not be able to END an
//! active login. Checking after the fact would only have changed which error
//! the operator saw; ignoring it, and continuing to wait, is what makes the
//! stray request harmless. The count of what was ignored is reported if the
//! wait then runs out, so a genuinely mismatched provider is still diagnosable
//! rather than silent.

use anyhow::{Context, Result, bail};
use percent_encoding::percent_decode_str;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};

/// The path the authorization server is told to redirect to.
pub const CALLBACK_PATH: &str = "/callback";

/// Cap on request-line bytes read from one connection. A callback URL carries
/// a code and a state; anything approaching this is not one.
const MAX_REQUEST_BYTES: u64 = 8 * 1024;

/// How long one connection gets to produce its request line.
///
/// The accept loop is sequential, so without this a socket that connects and
/// sends NOTHING holds the loop until the 120-second callback timeout — with
/// the real redirect already completed in the accept backlog, unread. That is
/// reachable adversarially (one idle `nc` to a loopback port denies the
/// sign-in) and accidentally (browsers open speculative preconnect sockets to
/// a navigation target and hold them idle).
const REQUEST_READ_TIMEOUT: Duration = Duration::from_secs(5);

/// How many non-callback requests are answered and ignored before giving up.
/// Bounded so a chatty client cannot keep the wait alive indefinitely — the
/// timeout is the other bound.
const MAX_UNRELATED_REQUESTS: usize = 8;

/// A bound loopback listener waiting for the authorization redirect.
pub struct Loopback {
    listener: TcpListener,
    port: u16,
    /// [`REQUEST_READ_TIMEOUT`] in production; shortened by tests so the
    /// silent-connection case does not cost five seconds per run.
    read_timeout: Duration,
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
        Ok(Loopback {
            listener,
            port,
            read_timeout: REQUEST_READ_TIMEOUT,
        })
    }

    /// Shorten the per-connection read deadline, so a test can exercise the
    /// silent-connection path without waiting out the production value.
    #[cfg(test)]
    fn with_read_timeout(mut self, read_timeout: Duration) -> Loopback {
        self.read_timeout = read_timeout;
        self
    }

    /// The `redirect_uri` to register with the authorization request. Literal
    /// `127.0.0.1` rather than `localhost`: the name can resolve to `::1`
    /// first, which is a different listener than the one bound above.
    pub fn redirect_uri(&self) -> String {
        format!("http://127.0.0.1:{}{CALLBACK_PATH}", self.port)
    }

    /// Wait for THIS sign-in's redirect, or fail once `timeout` elapses.
    ///
    /// Only a callback whose `state` equals `expected_state` is returned;
    /// anything else is answered and ignored (see the module docs).
    ///
    /// Consumes `self`, so the listener — and the port — is released when this
    /// returns, on the timeout path as much as on the success path.
    pub async fn wait(self, timeout: Duration, expected_state: &str) -> Result<Callback> {
        // Outside the timeout future, so a wait that runs out can still report
        // what it ignored. Atomic rather than `Cell` only because the future
        // must stay `Send` to be spawned.
        let ignored = AtomicUsize::new(0);
        match tokio::time::timeout(timeout, self.accept_callback(expected_state, &ignored)).await {
            Ok(result) => result,
            Err(_) => bail!(
                "no authorization response within {}s — the login was not completed{}",
                timeout.as_secs(),
                describe_ignored(ignored.load(Ordering::Relaxed))
            ),
        }
    }

    async fn accept_callback(
        &self,
        expected_state: &str,
        ignored: &AtomicUsize,
    ) -> Result<Callback> {
        for _ in 0..=MAX_UNRELATED_REQUESTS {
            let (mut stream, _peer) = self
                .listener
                .accept()
                .await
                .context("accept the login redirect")?;

            // A connection that goes quiet is noise, not this sign-in's
            // response: answer it and move on rather than letting it hold the
            // loop.
            let target =
                match tokio::time::timeout(self.read_timeout, read_request_target(&mut stream))
                    .await
                {
                    Ok(Ok(Some(target))) => target,
                    _ => {
                        respond(&mut stream, "400 Bad Request", "Malformed request.").await;
                        continue;
                    }
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
            // Membership in THIS sign-in, checked before either the code or
            // the error is looked at. A mismatch is treated as one more
            // unrelated request rather than as a failure, so nothing that can
            // reach this port can end the login.
            //
            // `state` is required on an error response too — RFC 6749 §4.1.2.1
            // requires it echoed whenever the request carried one — so a
            // refusal that omits it did not come from the provider we asked.
            if callback.state.as_deref() != Some(expected_state) {
                ignored.fetch_add(1, Ordering::Relaxed);
                respond(
                    &mut stream,
                    "400 Bad Request",
                    "This response does not belong to a sign-in started here.",
                )
                .await;
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
            "the login redirect never arrived after {MAX_UNRELATED_REQUESTS} unrelated requests on the callback port{}",
            describe_ignored(ignored.load(Ordering::Relaxed))
        )
    }
}

/// The clause naming callbacks that were refused for carrying the wrong
/// `state`, so a mismatch is diagnosable instead of looking like silence.
fn describe_ignored(count: usize) -> String {
    match count {
        0 => String::new(),
        1 => {
            " (one response arrived carrying a 'state' this sign-in did not issue, and was ignored)"
                .to_string()
        }
        many => format!(
            " ({many} responses arrived carrying a 'state' this sign-in did not issue, and were ignored)"
        ),
    }
}

/// Read the request target out of the first line, bounded. `None` when the
/// line is not a parseable `GET <target> HTTP/1.x`.
///
/// `read_until` under a `take`, rather than a byte at a time: the same
/// [`MAX_REQUEST_BYTES`] bound in one call instead of up to 8192 awaits.
async fn read_request_target(stream: &mut TcpStream) -> Result<Option<String>> {
    let mut buf = Vec::new();
    let mut reader = BufReader::new(stream).take(MAX_REQUEST_BYTES);
    // A client that hangs up mid-line is noise, not a failure of the wait:
    // report it as an unparseable line and keep listening.
    if reader.read_until(b'\n', &mut buf).await.is_err() {
        return Ok(None);
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
            .wait(Duration::from_millis(50), "the-state")
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

        let waiter =
            tokio::spawn(async move { loopback.wait(Duration::from_secs(5), "the-state").await });
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

        let waiter =
            tokio::spawn(async move { loopback.wait(Duration::from_secs(5), "the-state").await });

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

        let callback = reqwest::get(format!("{uri}?code=real&state=the-state"))
            .await
            .unwrap();
        assert!(callback.status().is_success());
        let received = waiter.await.unwrap().unwrap();
        assert_eq!(received.code.as_deref(), Some("real"));
    }

    /// The finding this exists for: anything on the machine can reach a
    /// loopback port, and a browser can be induced to fetch one cross-site. A
    /// planted `?error=` — or a planted code with a guessed state — must not be
    /// able to END an active login.
    #[tokio::test]
    async fn a_response_from_another_sign_in_cannot_end_this_one() {
        let loopback = Loopback::bind().await.unwrap();
        let uri = loopback.redirect_uri();

        let waiter =
            tokio::spawn(async move { loopback.wait(Duration::from_secs(5), "the-state").await });
        let http = reqwest::Client::new();

        // An error with no state at all, an error with someone else's state,
        // and a planted code — each answered, none accepted.
        for planted in [
            "?error=access_denied",
            "?error=access_denied&state=someone-elses",
            "?code=planted&state=someone-elses",
            "?code=planted",
        ] {
            let response = http.get(format!("{uri}{planted}")).send().await.unwrap();
            assert_eq!(response.status().as_u16(), 400, "{planted}");
            assert!(
                response.text().await.unwrap().contains("does not belong"),
                "{planted}"
            );
        }

        // The real redirect still completes.
        let _ = http
            .get(format!("{uri}?code=the-real-code&state=the-state"))
            .send()
            .await;
        let callback = waiter.await.unwrap().unwrap();
        assert_eq!(callback.code.as_deref(), Some("the-real-code"));
    }

    /// A mismatch that never resolves is not silence: the wait runs out and
    /// says what it refused, so a genuinely misconfigured provider is
    /// diagnosable.
    #[tokio::test]
    async fn a_wait_that_only_saw_foreign_responses_says_so() {
        let loopback = Loopback::bind().await.unwrap();
        let uri = loopback.redirect_uri();

        let waiter =
            tokio::spawn(async move { loopback.wait(Duration::from_millis(300), "mine").await });
        let _ = reqwest::get(format!("{uri}?code=c&state=not-mine")).await;

        let err = waiter.await.unwrap().unwrap_err().to_string();
        assert!(err.contains("no authorization response within"), "{err}");
        assert!(
            err.contains("did not issue"),
            "the ignored response must be reported: {err}"
        );
        assert!(!err.contains("not-mine"), "no value is echoed: {err}");
    }

    /// A connection that opens and SENDS NOTHING must not hold the sequential
    /// accept loop: without a per-connection deadline it blocks until the
    /// 120-second callback timeout, with the real redirect already completed in
    /// the accept backlog and unread. One idle `nc` to a loopback port would
    /// otherwise deny the sign-in.
    #[tokio::test]
    async fn a_silent_connection_does_not_hold_the_callback_behind_it() {
        let loopback = Loopback::bind()
            .await
            .unwrap()
            .with_read_timeout(Duration::from_millis(150));
        let uri = loopback.redirect_uri();
        let addr = uri
            .trim_start_matches("http://")
            .trim_end_matches("/callback")
            .to_string();

        // Accepted first, and never says anything — but deliberately kept
        // OPEN, which is what distinguishes this from the hang-up case.
        let idle = tokio::net::TcpStream::connect(&addr).await.unwrap();

        let waiter =
            tokio::spawn(async move { loopback.wait(Duration::from_secs(5), "the-state").await });
        // Let the loop accept the idle socket before the real redirect
        // arrives, so the ordering under test is the failing one.
        tokio::time::sleep(Duration::from_millis(50)).await;
        let started = std::time::Instant::now();
        let _ = reqwest::get(format!("{uri}?code=behind-the-idle&state=the-state")).await;

        let callback = waiter.await.unwrap().unwrap();
        assert_eq!(callback.code.as_deref(), Some("behind-the-idle"));
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "the callback waited behind the idle socket: {:?}",
            started.elapsed()
        );
        drop(idle);
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

        let waiter =
            tokio::spawn(async move { loopback.wait(Duration::from_secs(30), "the-state").await });
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

        let waiter =
            tokio::spawn(async move { loopback.wait(Duration::from_secs(5), "the-state").await });
        // The provider echoes `state` on an error response too (RFC 6749
        // §4.1.2.1), so a genuine refusal carries it — and only that reaches
        // the caller.
        let response = reqwest::get(format!(
            "{uri}?error=access_denied&error_description=user%20refused&state=the-state"
        ))
        .await
        .unwrap();
        assert!(response.text().await.unwrap().contains("Sign-in failed"));

        let callback = waiter.await.unwrap().unwrap();
        assert_eq!(callback.error.as_deref(), Some("access_denied"));
        assert_eq!(callback.error_description.as_deref(), Some("user refused"));
    }
}
