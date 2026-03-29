use axum::{
    extract::{Request, State},
    http::StatusCode,
    middleware::Next,
    response::{IntoResponse, Json, Response},
};
use jsonwebtoken::{decode, encode, DecodingKey, EncodingKey, Header, Validation};
use serde::{Deserialize, Serialize};

use crate::server::AppState;

const BCRYPT_COST: u32 = 10;
const TOKEN_EXPIRY_SECS: i64 = 24 * 3600;

/// JWT claims payload
#[derive(Debug, Serialize, Deserialize)]
pub struct Claims {
    /// Subject (email)
    pub sub: String,
    pub role: String,
    /// Expiry (unix timestamp)
    pub exp: usize,
    /// Issued at (unix timestamp)
    pub iat: usize,
}

/// Request body for POST /auth/login
#[derive(Debug, Deserialize)]
pub struct LoginRequest {
    pub email: String,
    pub password: String,
}

/// Auth configuration loaded from environment variables at startup
#[derive(Clone)]
pub struct AuthConfig {
    pub admin_email: String,
    pub admin_password_hash: String,
    pub jwt_secret: String,
}

impl AuthConfig {
    /// Load auth config from environment variables.
    ///
    /// Returns `Ok(None)` if `SKARDI_ADMIN_EMAIL` is not set (auth disabled).
    /// Returns an error if email is set but password is missing or hashing fails.
    pub fn from_env() -> anyhow::Result<Option<Self>> {
        let email = match std::env::var("SKARDI_ADMIN_EMAIL") {
            Ok(e) => e,
            Err(_) => {
                tracing::info!("SKARDI_ADMIN_EMAIL not set; authentication disabled");
                return Ok(None);
            }
        };

        let password = std::env::var("SKARDI_ADMIN_PASSWORD").map_err(|_| {
            anyhow::anyhow!(
                "SKARDI_ADMIN_PASSWORD must be set when SKARDI_ADMIN_EMAIL is configured"
            )
        })?;

        let jwt_secret = std::env::var("SKARDI_JWT_SECRET").unwrap_or_else(|_| {
            let secret = uuid::Uuid::new_v4().to_string();
            tracing::warn!(
                "SKARDI_JWT_SECRET not set; using a random secret \
                 (all tokens will be invalidated on server restart)"
            );
            secret
        });

        let password_hash = bcrypt::hash(&password, BCRYPT_COST)
            .map_err(|e| anyhow::anyhow!("Failed to hash admin password: {}", e))?;

        tracing::info!("Authentication enabled (admin: {})", email);

        Ok(Some(Self {
            admin_email: email,
            admin_password_hash: password_hash,
            jwt_secret,
        }))
    }
}

/// POST /auth/login
///
/// Authenticates with email + password and returns a JWT access token.
/// Returns 404 if authentication is not configured on this server.
pub async fn login(
    State(state): State<AppState>,
    Json(body): Json<LoginRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    let auth = match &state.auth {
        Some(a) => a,
        None => {
            return Err((
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({
                    "error": "Authentication is not configured on this server",
                    "error_type": "auth_not_configured"
                })),
            ));
        }
    };

    // Validate email first — same error message for both to prevent user enumeration
    let email_matches = body.email == auth.admin_email;
    let password_valid = bcrypt::verify(&body.password, &auth.admin_password_hash)
        .unwrap_or(false);

    if !email_matches || !password_valid {
        return Err((
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({
                "error": "Invalid email or password",
                "error_type": "invalid_credentials"
            })),
        ));
    }

    let now = chrono::Utc::now();
    let exp = now + chrono::Duration::seconds(TOKEN_EXPIRY_SECS);

    let claims = Claims {
        sub: body.email.clone(),
        role: "admin".to_string(),
        exp: exp.timestamp() as usize,
        iat: now.timestamp() as usize,
    };

    let token = encode(
        &Header::default(),
        &claims,
        &EncodingKey::from_secret(auth.jwt_secret.as_bytes()),
    )
    .map_err(|e| {
        tracing::error!("Failed to encode JWT: {}", e);
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({
                "error": "Failed to generate token",
                "error_type": "internal_error"
            })),
        )
    })?;

    tracing::info!("Successful login for: {}", body.email);

    Ok(Json(serde_json::json!({
        "access_token": token,
        "token_type": "Bearer",
        "expires_in": TOKEN_EXPIRY_SECS
    })))
}

/// JWT authentication middleware.
///
/// When auth is disabled (`state.auth` is `None`), all requests pass through.
/// When auth is enabled, requires a valid `Authorization: Bearer <token>` header.
pub async fn jwt_auth_middleware(
    State(state): State<AppState>,
    request: Request,
    next: Next,
) -> Response {
    let Some(auth) = &state.auth else {
        return next.run(request).await;
    };

    let token = request
        .headers()
        .get("Authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .map(str::to_string);

    let Some(token) = token else {
        return (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({
                "error": "Missing authorization token",
                "error_type": "missing_token"
            })),
        )
            .into_response();
    };

    match decode::<Claims>(
        &token,
        &DecodingKey::from_secret(auth.jwt_secret.as_bytes()),
        &Validation::default(),
    ) {
        Ok(_) => next.run(request).await,
        Err(_) => (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({
                "error": "Invalid or expired token",
                "error_type": "invalid_token"
            })),
        )
            .into_response(),
    }
}
