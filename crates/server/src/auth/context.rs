//! Per-request auth context used by the pipeline + jobs handlers.
//!
//! There are two ways to authenticate against the server:
//!
//! 1. **Browser session** — cookie or `Authorization: Bearer <session>` issued
//!    by better-auth. The user's coarse role (`admin` / `operator` / `viewer`)
//!    is mapped to an implicit scope set via [`super::scope::scopes_for_role`].
//! 2. **API key** — `Authorization: Bearer skardi_…` minted via the
//!    `/api/keys` endpoints. The token's stored `scopes` list is the
//!    granted set; the user's role is ignored.
//!
//! When the auth layer is disabled (`AUTH_MODE=NO_AUTH`, the default),
//! every request resolves to a synthetic admin context so the existing
//! "no auth at all" behaviour is preserved.
//!
//! Handlers should not call this module's `extract_auth_context` directly;
//! they should call [`require_scope`] which composes extraction +
//! authorization and produces the final 401/403 response on failure.

use axum::{
    body::Body,
    http::{HeaderMap, Response, StatusCode},
};
use better_auth::{SessionOps, UserOps};
// `UserOps::get_user_by_id` and `SessionOps::get_session` are the two
// trait methods we lean on. Both come in via the `better_auth` re-export.
use cookie::Cookie;

use super::api_keys::TOKEN_PREFIX;
use super::scope::{any_scope_matches, scopes_for_role};
use crate::server::AppState;

/// What kind of credential satisfied this request — useful for logging
/// and for handlers that want to audit "did this come from a CLI key or
/// a logged-in user".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthKind {
    /// `AUTH_MODE=NO_AUTH` — no credential was required; we synthesise
    /// admin scopes so existing handlers continue to work.
    Disabled,
    /// Better-auth browser session (cookie or session token).
    Session,
    /// `Authorization: Bearer skardi_…` resolved against the api_keys store.
    ApiKey,
}

/// What we know about the caller. `scopes` is the fully resolved grant
/// list — handlers only need to call [`super::scope::any_scope_matches`]
/// against it, no further role lookup required.
#[derive(Debug, Clone)]
pub struct AuthContext {
    pub user_id: String,
    pub scopes: Vec<String>,
    pub kind: AuthKind,
}

impl AuthContext {
    /// Built when auth is disabled — admin scope so every check passes.
    fn anonymous_admin() -> Self {
        Self {
            user_id: "anonymous".to_string(),
            scopes: vec!["*".to_string()],
            kind: AuthKind::Disabled,
        }
    }
}

/// Extract bearer token (or session cookie) from the request headers.
/// Returns `None` when no candidate credential is present — caller
/// decides whether that is a 401 or a pass-through.
fn read_bearer(headers: &HeaderMap, session_cookie_name: &str) -> Option<String> {
    if let Some(s) = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "))
        .map(str::to_string)
    {
        return Some(s);
    }
    let cookie_header = headers.get("cookie").and_then(|v| v.to_str().ok())?;
    for c in Cookie::split_parse(cookie_header).flatten() {
        let c: Cookie<'_> = c;
        if c.name() == session_cookie_name && !c.value().is_empty() {
            return Some(c.value().to_string());
        }
    }
    None
}

/// Resolve the request to an [`AuthContext`] without yet checking any
/// scope. Returns `Err` when the credential is missing/invalid; the
/// caller turns that into a 401 response.
pub async fn extract_auth_context(
    state: &AppState,
    headers: &HeaderMap,
) -> Result<AuthContext, AuthError> {
    let auth = match state.auth_layer.as_better_auth() {
        Some(a) => a,
        None => return Ok(AuthContext::anonymous_admin()),
    };

    let cookie_name = auth.config().session.cookie_name.clone();
    let token = read_bearer(headers, &cookie_name).ok_or(AuthError::MissingCredential)?;

    // API keys are recognisable by their `skardi_` prefix, so we don't
    // need to probe the api_keys store on every session lookup.
    if token.starts_with(TOKEN_PREFIX) {
        let store = state
            .api_keys
            .as_ref()
            .ok_or(AuthError::ApiKeysUnavailable)?;
        let record = store
            .lookup_by_token(&token)
            .await
            .map_err(|e| AuthError::Internal(e.to_string()))?
            .ok_or(AuthError::InvalidCredential)?;
        if record.is_expired_or_revoked(chrono::Utc::now()) {
            return Err(AuthError::InvalidCredential);
        }
        return Ok(AuthContext {
            user_id: record.user_id.clone(),
            scopes: record.scopes(),
            kind: AuthKind::ApiKey,
        });
    }

    // Otherwise: better-auth session.
    let session = auth
        .database()
        .get_session(&token)
        .await
        .map_err(|e| AuthError::Internal(e.to_string()))?
        .ok_or(AuthError::InvalidCredential)?;
    if session.expires_at <= chrono::Utc::now() || !session.active {
        return Err(AuthError::InvalidCredential);
    }
    let user = auth
        .database()
        .get_user_by_id(&session.user_id)
        .await
        .map_err(|e| AuthError::Internal(e.to_string()))?
        .ok_or(AuthError::InvalidCredential)?;
    let scopes = scopes_for_role(user.role.as_deref());
    Ok(AuthContext {
        user_id: session.user_id.clone(),
        scopes,
        kind: AuthKind::Session,
    })
}

/// Convenience wrapper used by every protected handler:
///
/// 1. Extract the auth context (401 on failure).
/// 2. Confirm at least one granted scope matches `required` (403 on failure).
///
/// Returns the resolved context on success so handlers can log
/// `user_id`/`kind` if they want; ignore the value otherwise.
pub async fn require_scope(
    state: &AppState,
    headers: &HeaderMap,
    required: &str,
) -> Result<AuthContext, Response<Body>> {
    let ctx = extract_auth_context(state, headers)
        .await
        .map_err(|e| e.into_response())?;
    if any_scope_matches(&ctx.scopes, required) {
        Ok(ctx)
    } else {
        Err(forbidden_response(required, &ctx.scopes))
    }
}

/// Errors surfaced by [`extract_auth_context`]. They map directly onto
/// the HTTP responses the handler returns; no further inspection needed.
#[derive(Debug)]
pub enum AuthError {
    MissingCredential,
    InvalidCredential,
    ApiKeysUnavailable,
    Internal(String),
}

impl AuthError {
    fn into_response(self) -> Response<Body> {
        let (status, msg) = match self {
            AuthError::MissingCredential => (StatusCode::UNAUTHORIZED, "Authentication required"),
            AuthError::InvalidCredential => {
                (StatusCode::UNAUTHORIZED, "Invalid or expired credential")
            }
            AuthError::ApiKeysUnavailable => (
                StatusCode::SERVICE_UNAVAILABLE,
                "API key store is not available",
            ),
            AuthError::Internal(_) => (StatusCode::INTERNAL_SERVER_ERROR, "Internal auth error"),
        };
        let body = serde_json::json!({ "error": msg }).to_string();
        Response::builder()
            .status(status)
            .header("Content-Type", "application/json")
            .body(Body::from(body))
            .expect("static response always builds")
    }
}

fn forbidden_response(required: &str, granted: &[String]) -> Response<Body> {
    let body = serde_json::json!({
        "error": "Insufficient scope",
        "required_scope": required,
        "granted_scopes": granted,
    })
    .to_string();
    Response::builder()
        .status(StatusCode::FORBIDDEN)
        .header("Content-Type", "application/json")
        .body(Body::from(body))
        .expect("static response always builds")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::api_keys::ApiKeyStore;
    use crate::auth::layer::AuthLayer;
    use crate::auth::mode::AuthMode;
    use crate::config::{CliArgs, ServerConfig};
    use crate::metrics::PipelineMetrics;
    use crate::semantics::SemanticsRegistry;
    use crate::server::AppState;
    use datafusion::prelude::SessionContext;
    use skardi::engine::datafusion::DataFusionEngine;
    use std::path::PathBuf;
    use std::sync::{Arc, RwLock};
    use tempfile::TempDir;

    fn empty_app_state(auth_layer: AuthLayer, api_keys: Option<ApiKeyStore>) -> AppState {
        let config = ServerConfig {
            pipelines: Default::default(),
            jobs: Default::default(),
            data_sources: vec![],
            semantics: SemanticsRegistry::default(),
            args: CliArgs {
                pipeline_path: Some(PathBuf::from("p.yaml")),
                jobs_path: None,
                jobs_db_path: None,
                ctx_file: None,
                semantics_path: None,
                port: 8080,
            },
        };
        let session_ctx = Arc::new(SessionContext::new());
        let engine = Arc::new(DataFusionEngine::new_with_arc(session_ctx.clone()));
        AppState {
            config: Arc::new(RwLock::new(config)),
            engine,
            session_ctx,
            metrics: PipelineMetrics::new(),
            auth_layer,
            jobs: None,
            api_keys,
        }
    }

    async fn fresh_store() -> (ApiKeyStore, TempDir) {
        let dir = TempDir::new().unwrap();
        let store = ApiKeyStore::open(&dir.path().join("k.db")).await.unwrap();
        (store, dir)
    }

    #[tokio::test]
    async fn no_auth_layer_synthesises_admin_context() {
        let state = empty_app_state(AuthLayer::None, None);
        let ctx = extract_auth_context(&state, &HeaderMap::new())
            .await
            .unwrap();
        assert_eq!(ctx.kind, AuthKind::Disabled);
        assert!(any_scope_matches(&ctx.scopes, "anything:goes"));
    }

    #[tokio::test]
    async fn no_auth_layer_passes_require_scope() {
        let state = empty_app_state(AuthLayer::None, None);
        require_scope(&state, &HeaderMap::new(), "pipeline:execute:foo")
            .await
            .unwrap();
    }

    async fn make_better_auth_state(api_keys: Option<ApiKeyStore>) -> AppState {
        unsafe {
            std::env::set_var("AUTH_SECRET", "test-secret-that-is-at-least-32-characters!");
            std::env::set_var("AUTH_DB_PATH", ":memory:");
            std::env::remove_var("AUTH_BASE_URL");
        }
        let layer = AuthLayer::build(&AuthMode::BetterAuthDieselSqlite)
            .await
            .unwrap();
        empty_app_state(layer, api_keys)
    }

    #[tokio::test]
    async fn missing_token_is_401() {
        let state = make_better_auth_state(None).await;
        let err = require_scope(&state, &HeaderMap::new(), "pipeline:read:*")
            .await
            .unwrap_err();
        assert_eq!(err.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn unknown_api_key_is_401() {
        let (store, _g) = fresh_store().await;
        let state = make_better_auth_state(Some(store)).await;
        let mut headers = HeaderMap::new();
        headers.insert("authorization", "Bearer skardi_unknown".parse().unwrap());
        let err = require_scope(&state, &headers, "pipeline:read:*")
            .await
            .unwrap_err();
        assert_eq!(err.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn api_key_grants_match_scope() {
        let (store, _g) = fresh_store().await;
        let (_rec, token) = store
            .create_key("u1", "ci", &["pipeline:execute:foo".to_string()], None)
            .await
            .unwrap();
        let state = make_better_auth_state(Some(store)).await;
        let mut headers = HeaderMap::new();
        headers.insert(
            "authorization",
            format!("Bearer {}", token).parse().unwrap(),
        );
        let ctx = require_scope(&state, &headers, "pipeline:execute:foo")
            .await
            .unwrap();
        assert_eq!(ctx.kind, AuthKind::ApiKey);
        assert_eq!(ctx.user_id, "u1");
    }

    #[tokio::test]
    async fn api_key_without_required_scope_is_403() {
        let (store, _g) = fresh_store().await;
        let (_rec, token) = store
            .create_key("u1", "ci", &["pipeline:read:*".to_string()], None)
            .await
            .unwrap();
        let state = make_better_auth_state(Some(store)).await;
        let mut headers = HeaderMap::new();
        headers.insert(
            "authorization",
            format!("Bearer {}", token).parse().unwrap(),
        );
        let err = require_scope(&state, &headers, "pipeline:execute:foo")
            .await
            .unwrap_err();
        assert_eq!(err.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn revoked_api_key_is_401() {
        let (store, _g) = fresh_store().await;
        let (rec, token) = store
            .create_key("u1", "ci", &["pipeline:execute:foo".to_string()], None)
            .await
            .unwrap();
        store.revoke(&rec.id).await.unwrap();
        let state = make_better_auth_state(Some(store)).await;
        let mut headers = HeaderMap::new();
        headers.insert(
            "authorization",
            format!("Bearer {}", token).parse().unwrap(),
        );
        let err = require_scope(&state, &headers, "pipeline:execute:foo")
            .await
            .unwrap_err();
        assert_eq!(err.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn session_with_admin_role_passes_any_scope() {
        use better_auth::types_mod::{CreateSession, CreateUser};

        let (store, _g) = fresh_store().await;
        let state = make_better_auth_state(Some(store)).await;
        let auth = state.auth_layer.as_better_auth().unwrap();

        let user = auth
            .database()
            .create_user(CreateUser {
                name: Some("admin".into()),
                email: Some("admin@test.com".into()),
                password: Some("password123".into()),
                role: Some("admin".into()),
                ..Default::default()
            })
            .await
            .unwrap();
        let session = auth
            .database()
            .create_session(CreateSession {
                user_id: user.id.clone(),
                expires_at: chrono::Utc::now() + chrono::Duration::hours(1),
                ip_address: None,
                user_agent: None,
                impersonated_by: None,
                active_organization_id: None,
            })
            .await
            .unwrap();

        let mut headers = HeaderMap::new();
        headers.insert(
            "authorization",
            format!("Bearer {}", session.token).parse().unwrap(),
        );
        let ctx = require_scope(&state, &headers, "keys:manage")
            .await
            .unwrap();
        assert_eq!(ctx.kind, AuthKind::Session);
        assert_eq!(ctx.user_id, user.id);
    }

    #[tokio::test]
    async fn session_without_role_is_403_on_protected_routes() {
        use better_auth::types_mod::{CreateSession, CreateUser};

        let state = make_better_auth_state(None).await;
        let auth = state.auth_layer.as_better_auth().unwrap();
        let user = auth
            .database()
            .create_user(CreateUser {
                name: Some("user".into()),
                email: Some("u@test.com".into()),
                password: Some("password123".into()),
                ..Default::default()
            })
            .await
            .unwrap();
        let session = auth
            .database()
            .create_session(CreateSession {
                user_id: user.id.clone(),
                expires_at: chrono::Utc::now() + chrono::Duration::hours(1),
                ip_address: None,
                user_agent: None,
                impersonated_by: None,
                active_organization_id: None,
            })
            .await
            .unwrap();

        let mut headers = HeaderMap::new();
        headers.insert(
            "authorization",
            format!("Bearer {}", session.token).parse().unwrap(),
        );
        let err = require_scope(&state, &headers, "pipeline:execute:foo")
            .await
            .unwrap_err();
        assert_eq!(err.status(), StatusCode::FORBIDDEN);
    }
}
