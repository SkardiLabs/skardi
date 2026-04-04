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
                let secret = std::env::var("AUTH_SECRET").map_err(|_| {
                    anyhow!(
                        "AUTH_SECRET environment variable must be set when \
                         AUTH_MODE=BETTER_AUTH_IN_MEMORY"
                    )
                })?;

                // Prefer an explicit AUTH_BASE_URL; fall back to localhost:{PORT}.
                // In production set AUTH_BASE_URL to the server's public URL so
                // that cookies, redirects, and absolute links work correctly.
                let base_url = std::env::var("AUTH_BASE_URL").unwrap_or_else(|_| {
                    let port = std::env::var("PORT").unwrap_or_else(|_| "8080".to_string());
                    format!("http://localhost:{}", port)
                });

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

#[cfg(test)]
mod tests {
    use super::*;

    // ─── AuthLayer::None ────────────────────────────────────────────────

    #[tokio::test]
    async fn build_no_auth() {
        let layer = AuthLayer::build(&AuthMode::NoAuth).await.unwrap();
        assert!(!layer.is_enabled());
        assert!(layer.as_better_auth().is_none());
    }

    #[test]
    fn clone_none_variant() {
        let layer = AuthLayer::None;
        let cloned = layer.clone();
        assert!(!cloned.is_enabled());
        assert!(cloned.as_better_auth().is_none());
    }

    // ─── AuthLayer::BetterAuthInMemory ──────────────────────────────────

    #[tokio::test]
    async fn build_better_auth_missing_secret_errors() {
        unsafe { std::env::remove_var("AUTH_SECRET") };
        let result = AuthLayer::build(&AuthMode::BetterAuthInMemory).await;
        match result {
            Err(e) => assert!(
                e.to_string().contains("AUTH_SECRET"),
                "error should mention AUTH_SECRET, got: {e}"
            ),
            Ok(_) => panic!("expected Err when AUTH_SECRET is unset"),
        }
    }

    #[tokio::test]
    async fn build_better_auth_success() {
        unsafe {
            std::env::set_var("AUTH_SECRET", "test-secret-that-is-at-least-32-characters!");
            std::env::remove_var("AUTH_BASE_URL");
            std::env::remove_var("PORT");
        }

        let layer = AuthLayer::build(&AuthMode::BetterAuthInMemory)
            .await
            .unwrap();
        assert!(layer.is_enabled());
        assert!(layer.as_better_auth().is_some());

        unsafe { std::env::remove_var("AUTH_SECRET") };
    }

    #[tokio::test]
    async fn build_better_auth_respects_auth_base_url() {
        unsafe {
            std::env::set_var("AUTH_SECRET", "test-secret-that-is-at-least-32-characters!");
            std::env::set_var("AUTH_BASE_URL", "https://example.com");
        }

        let layer = AuthLayer::build(&AuthMode::BetterAuthInMemory)
            .await
            .unwrap();
        let auth = layer.as_better_auth().unwrap();
        assert_eq!(auth.config().base_url, "https://example.com");

        unsafe {
            std::env::remove_var("AUTH_SECRET");
            std::env::remove_var("AUTH_BASE_URL");
        }
    }

    #[tokio::test]
    async fn clone_better_auth_shares_arc() {
        unsafe {
            std::env::set_var("AUTH_SECRET", "test-secret-that-is-at-least-32-characters!");
            std::env::remove_var("AUTH_BASE_URL");
        }

        let layer = AuthLayer::build(&AuthMode::BetterAuthInMemory)
            .await
            .unwrap();
        let cloned = layer.clone();

        assert!(cloned.is_enabled());
        let ptr_a = Arc::as_ptr(layer.as_better_auth().unwrap());
        let ptr_b = Arc::as_ptr(cloned.as_better_auth().unwrap());
        assert_eq!(ptr_a, ptr_b, "clone should share the same Arc");

        unsafe { std::env::remove_var("AUTH_SECRET") };
    }
}
