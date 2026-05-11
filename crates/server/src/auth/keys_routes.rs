//! `/api/keys` endpoints for managing bearer tokens.
//!
//! All four endpoints require a session-authenticated admin OR an API
//! key carrying `keys:manage`. Concretely that means a logged-in user
//! whose better-auth `role` is `admin` (which maps to `*`) or a token
//! minted with `["keys:manage"]` in its scopes — admins can bootstrap
//! the first such token via the dashboard.
//!
//! Endpoints:
//!
//! * `POST   /api/keys`       — mint a new key. Returns the raw token ONCE.
//! * `GET    /api/keys`       — list keys (admin sees all; otherwise own only).
//! * `DELETE /api/keys/:id`   — revoke a key.

use axum::{
    Json,
    body::Body,
    extract::{Path, State},
    http::{HeaderMap, Response, StatusCode},
    response::IntoResponse,
};
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};

use super::api_keys::ApiKeyRecord;
use super::context::require_scope;
use super::scope::any_scope_matches;
use crate::server::AppState;

/// Request body for `POST /api/keys`. `expires_in_days` is the simple
/// knob; callers that want a precise instant can pass `expires_at`
/// directly. If both are set, `expires_at` wins.
#[derive(Debug, Deserialize)]
pub struct CreateKeyRequest {
    pub name: String,
    pub scopes: Vec<String>,
    /// Optional shorthand. Days from now until the key stops working.
    #[serde(default)]
    pub expires_in_days: Option<i64>,
    /// RFC-3339 timestamp. Wins when both knobs are set.
    #[serde(default)]
    pub expires_at: Option<DateTime<Utc>>,
    /// Override the owner. Only honored for admin callers — non-admins
    /// always get a key minted under their own user id.
    #[serde(default)]
    pub user_id: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct CreateKeyResponse {
    pub id: String,
    pub user_id: String,
    pub name: String,
    pub scopes: Vec<String>,
    pub created_at: String,
    pub expires_at: Option<String>,
    /// The raw bearer token. Surfaced exactly once — not retrievable later.
    pub token: String,
}

#[derive(Debug, Serialize)]
pub struct KeySummary {
    pub id: String,
    pub user_id: String,
    pub name: String,
    pub scopes: Vec<String>,
    pub created_at: String,
    pub expires_at: Option<String>,
    pub revoked_at: Option<String>,
}

impl From<&ApiKeyRecord> for KeySummary {
    fn from(r: &ApiKeyRecord) -> Self {
        Self {
            id: r.id.clone(),
            user_id: r.user_id.clone(),
            name: r.name.clone(),
            scopes: r.scopes(),
            created_at: r.created_at.clone(),
            expires_at: r.expires_at.clone(),
            revoked_at: r.revoked_at.clone(),
        }
    }
}

fn err_response(status: StatusCode, msg: &str) -> Response<Body> {
    let body = serde_json::json!({ "error": msg }).to_string();
    Response::builder()
        .status(status)
        .header("Content-Type", "application/json")
        .body(Body::from(body))
        .expect("static response always builds")
}

/// `POST /api/keys`
pub async fn create_key(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<CreateKeyRequest>,
) -> Response<Body> {
    let ctx = match require_scope(&state, &headers, "keys:manage").await {
        Ok(c) => c,
        Err(resp) => return resp,
    };
    let Some(store) = state.api_keys.as_ref() else {
        return err_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "API key store is not available",
        );
    };

    let is_admin = any_scope_matches(&ctx.scopes, "*");
    let owner_id = match (&req.user_id, is_admin) {
        (Some(uid), true) => uid.clone(),
        (Some(_), false) => {
            // A non-admin caller asked for a key on behalf of another
            // user — refuse rather than silently rebinding it.
            return err_response(
                StatusCode::FORBIDDEN,
                "Only admin callers can mint keys for other users",
            );
        }
        (None, _) => ctx.user_id.clone(),
    };

    if req.scopes.is_empty() {
        return err_response(
            StatusCode::BAD_REQUEST,
            "scopes must contain at least one entry",
        );
    }
    // Reject obviously bad scope strings up front so the failure mode
    // is "400 with a message" rather than "200 and the key never works".
    for s in &req.scopes {
        if s.trim().is_empty() {
            return err_response(StatusCode::BAD_REQUEST, "scopes must not be empty strings");
        }
    }

    let expires_at = req
        .expires_at
        .or_else(|| req.expires_in_days.map(|d| Utc::now() + Duration::days(d)));

    let result = store
        .create_key(&owner_id, &req.name, &req.scopes, expires_at)
        .await;
    let (record, raw) = match result {
        Ok(t) => t,
        Err(e) => {
            tracing::error!("create_key failed: {e}");
            return err_response(StatusCode::INTERNAL_SERVER_ERROR, "Failed to create key");
        }
    };

    tracing::info!(
        target: "skardi::auth",
        actor = %ctx.user_id,
        actor_kind = ?ctx.kind,
        new_key_id = %record.id,
        owner = %owner_id,
        "minted api key"
    );

    let scopes = record.scopes();
    let body = CreateKeyResponse {
        id: record.id,
        user_id: record.user_id,
        name: record.name,
        scopes,
        created_at: record.created_at,
        expires_at: record.expires_at,
        token: raw,
    };
    (StatusCode::CREATED, Json(body)).into_response()
}

/// `GET /api/keys`
///
/// Admins (`*` scope) see every key; everyone else sees their own.
/// Non-admin users *can* still call this — the result is just filtered
/// to their own keys. Useful for "show my CI tokens" UIs.
pub async fn list_keys(State(state): State<AppState>, headers: HeaderMap) -> Response<Body> {
    let ctx = match require_scope(&state, &headers, "keys:manage").await {
        Ok(c) => c,
        Err(resp) => return resp,
    };
    let Some(store) = state.api_keys.as_ref() else {
        return err_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "API key store is not available",
        );
    };

    let scope = if any_scope_matches(&ctx.scopes, "*") {
        None
    } else {
        Some(ctx.user_id.as_str())
    };
    let keys = match store.list_keys(scope).await {
        Ok(v) => v,
        Err(e) => {
            tracing::error!("list_keys failed: {e}");
            return err_response(StatusCode::INTERNAL_SERVER_ERROR, "Failed to list keys");
        }
    };
    let body: Vec<KeySummary> = keys.iter().map(KeySummary::from).collect();
    let count = body.len();
    (
        StatusCode::OK,
        Json(serde_json::json!({
            "success": true,
            "keys": body,
            "count": count,
        })),
    )
        .into_response()
}

/// `DELETE /api/keys/:id`
pub async fn revoke_key(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(key_id): Path<String>,
) -> Response<Body> {
    let ctx = match require_scope(&state, &headers, "keys:manage").await {
        Ok(c) => c,
        Err(resp) => return resp,
    };
    let Some(store) = state.api_keys.as_ref() else {
        return err_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "API key store is not available",
        );
    };

    // Look up the key first so we can confirm ownership before revoking
    // — non-admin callers must not be able to revoke each other's keys
    // by guessing UUIDs.
    let owner_check = match store.list_keys(None).await {
        Ok(v) => v.into_iter().find(|r| r.id == key_id),
        Err(e) => {
            tracing::error!("revoke_key lookup failed: {e}");
            return err_response(StatusCode::INTERNAL_SERVER_ERROR, "Failed to revoke key");
        }
    };
    let Some(record) = owner_check else {
        return err_response(StatusCode::NOT_FOUND, "API key not found");
    };
    let is_admin = any_scope_matches(&ctx.scopes, "*");
    if !is_admin && record.user_id != ctx.user_id {
        return err_response(
            StatusCode::FORBIDDEN,
            "You can only revoke keys that you own",
        );
    }

    let revoked = match store.revoke(&key_id).await {
        Ok(b) => b,
        Err(e) => {
            tracing::error!("revoke_key failed: {e}");
            return err_response(StatusCode::INTERNAL_SERVER_ERROR, "Failed to revoke key");
        }
    };
    tracing::info!(
        target: "skardi::auth",
        actor = %ctx.user_id,
        actor_kind = ?ctx.kind,
        revoked_key_id = %key_id,
        "revoked api key"
    );
    (
        StatusCode::OK,
        Json(serde_json::json!({
            "success": true,
            "id": key_id,
            "revoked": revoked,
        })),
    )
        .into_response()
}

/// True when this token's auth context is allowed to bootstrap a key.
/// Exported for tests; not part of the public router surface.
#[cfg(test)]
fn _can_manage_keys(ctx: &super::context::AuthContext) -> bool {
    matches!(ctx.kind, super::context::AuthKind::Disabled)
        || any_scope_matches(&ctx.scopes, "keys:manage")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::api_keys::ApiKeyStore;
    use crate::auth::context::{AuthContext, AuthKind};

    #[test]
    fn admin_can_manage_keys() {
        let ctx = AuthContext {
            user_id: "u".into(),
            scopes: vec!["*".into()],
            kind: AuthKind::Session,
        };
        assert!(_can_manage_keys(&ctx));
    }

    #[test]
    fn explicit_keys_manage_grants_management() {
        let ctx = AuthContext {
            user_id: "u".into(),
            scopes: vec!["keys:manage".into()],
            kind: AuthKind::ApiKey,
        };
        assert!(_can_manage_keys(&ctx));
    }

    #[test]
    fn unrelated_scope_does_not_grant_management() {
        let ctx = AuthContext {
            user_id: "u".into(),
            scopes: vec!["pipeline:execute:*".into()],
            kind: AuthKind::ApiKey,
        };
        assert!(!_can_manage_keys(&ctx));
    }

    #[tokio::test]
    async fn create_then_list_then_revoke_records_show_in_store() {
        // Drives the store directly — the HTTP layer is tested via the
        // axum router integration test elsewhere.
        let dir = tempfile::TempDir::new().unwrap();
        let store = ApiKeyStore::open(&dir.path().join("k.db")).await.unwrap();
        let (rec, _token) = store
            .create_key("u1", "ci", &["pipeline:execute:foo".to_string()], None)
            .await
            .unwrap();

        let listed = store.list_keys(Some("u1")).await.unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].id, rec.id);

        assert!(store.revoke(&rec.id).await.unwrap());
        let listed_after = store.list_keys(Some("u1")).await.unwrap();
        assert!(listed_after[0].revoked_at.is_some());
    }

    #[test]
    fn key_summary_round_trips_to_json() {
        let rec = ApiKeyRecord {
            id: "k1".into(),
            user_id: "u1".into(),
            name: "ci".into(),
            scopes_json: "[\"pipeline:read:*\"]".into(),
            created_at: "2026-05-11T00:00:00Z".into(),
            expires_at: None,
            revoked_at: None,
        };
        let summary: KeySummary = (&rec).into();
        let v: serde_json::Value = serde_json::to_value(&summary).unwrap();
        assert_eq!(v["id"], "k1");
        assert_eq!(v["scopes"][0], "pipeline:read:*");
    }
}
