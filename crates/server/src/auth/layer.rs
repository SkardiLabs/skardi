use std::sync::Arc;

use anyhow::{anyhow, Result};
use better_auth::plugins::EmailPasswordPlugin;
use better_auth::{AuthBuilder, AuthConfig, BetterAuth, MemoryDatabaseAdapter};

use super::mode::AuthMode;

/// The active authentication layer, stored in [`crate::server::AppState`].
///
/// Wraps the concrete `BetterAuth<MemoryDatabaseAdapter>` instance in an
/// `Arc` so that `AppState` (and all Axum handler clones) share a single
/// live auth instance without requiring `BetterAuth` itself to be `Clone`.
pub enum AuthLayer {
    /// No authentication — pipeline endpoints are publicly accessible.
    None,
    /// better-auth backed by a shared in-memory database.
    BetterAuthInMemory(Arc<BetterAuth<MemoryDatabaseAdapter>>),
}

// Manual Clone: clone the Arc (reference count), never the BetterAuth itself.
impl Clone for AuthLayer {
    fn clone(&self) -> Self {
        match self {
            AuthLayer::None => AuthLayer::None,
            AuthLayer::BetterAuthInMemory(arc) => AuthLayer::BetterAuthInMemory(Arc::clone(arc)),
        }
    }
}

impl AuthLayer {
    /// Construct the appropriate `AuthLayer` for the given [`AuthMode`].
    pub async fn build(mode: &AuthMode) -> Result<Self> {
        match mode {
            AuthMode::NoAuth => Ok(AuthLayer::None),
            AuthMode::BetterAuthInMemory => {
                let secret = std::env::var("AUTH_SECRET")
                    .unwrap_or_else(|_| "skardi-demo-secret-key-32-chars!!!!".to_string());

                let port = std::env::var("PORT").unwrap_or_else(|_| "8080".to_string());
                let base_url = format!("http://localhost:{}", port);

                let config = AuthConfig::new(secret)
                    .base_url(base_url)
                    .base_path("/api/auth");

                let auth = AuthBuilder::new(config)
                    .database(MemoryDatabaseAdapter::new())
                    .plugin(EmailPasswordPlugin::new().enable_signup(true))
                    .build()
                    .await
                    .map_err(|e| anyhow!("Failed to initialise BetterAuth: {}", e))?;

                tracing::info!(
                    "BetterAuth initialised (in-memory). \
                     Sign-up: POST /api/auth/sign-up/email  \
                     Sign-in: POST /api/auth/sign-in/email"
                );

                Ok(AuthLayer::BetterAuthInMemory(Arc::new(auth)))
            }
        }
    }

    pub fn is_enabled(&self) -> bool {
        !matches!(self, AuthLayer::None)
    }

    /// Returns a reference to the inner `BetterAuth` instance, if present.
    pub fn as_better_auth(&self) -> Option<&Arc<BetterAuth<MemoryDatabaseAdapter>>> {
        match self {
            AuthLayer::BetterAuthInMemory(a) => Some(a),
            AuthLayer::None => Option::None,
        }
    }
}
