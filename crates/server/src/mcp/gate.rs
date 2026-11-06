//! `/mcp`-specific transport middleware: every inbound request authenticates
//! via `verify_session` BEFORE rmcp creates any session state — an anonymous
//! `initialize` is a transport-level 401, not a retained session, and missing
//! credentials surface as the 401 host credential flows key on (not as tool
//! errors inside HTTP 200s). Wraps only the nested MCP service; synthetic
//! dispatches never traverse it.
//!
//! Bearer only: `verify_session`'s cookie fallback would admit callers whose
//! every tool call then fails handler-level auth (synthetic requests forward
//! only `Authorization`), so the gate strips `cookie` before the check —
//! token validation stays single-home while the accepted carrier narrows to
//! the one MCP hosts actually send. This is also what keeps the open
//! `allowed_origins` posture honest: a browser's ambient cookie cannot
//! authenticate `/mcp`, so there is no CSRF-shaped surface for Origin to
//! guard. On a no-auth deployment `verify_session` always allows and the
//! gate is a no-op.

use std::convert::Infallible;
use std::task::{Context, Poll};

use axum::body::Body;
use axum::http::{Request, header};
use axum::response::{IntoResponse, Response};
use futures::future::BoxFuture;

use crate::auth::routes::verify_session;
use crate::server::AppState;

#[derive(Clone)]
pub(crate) struct SessionGate<S> {
    inner: S,
    state: AppState,
}

impl<S> SessionGate<S> {
    pub(crate) fn new(inner: S, state: AppState) -> Self {
        SessionGate { inner, state }
    }
}

impl<S> tower::Service<Request<Body>> for SessionGate<S>
where
    S: tower::Service<Request<Body>, Error = Infallible> + Clone + Send + 'static,
    S::Response: IntoResponse,
    S::Future: Send,
{
    type Response = Response;
    type Error = Infallible;
    type Future = BoxFuture<'static, Result<Response, Infallible>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Infallible>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Request<Body>) -> Self::Future {
        // Standard tower pattern: take the service that was polled ready,
        // leave a fresh clone behind.
        let clone = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);
        let state = self.state.clone();
        Box::pin(async move {
            let mut headers = req.headers().clone();
            headers.remove(header::COOKIE);
            if let Err(unauthorized) = verify_session(&state, &headers).await {
                return Ok(unauthorized.into_response());
            }
            Ok(inner
                .call(req)
                .await
                .unwrap_or_else(|infallible| match infallible {})
                .into_response())
        })
    }
}
