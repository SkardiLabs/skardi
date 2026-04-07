use std::sync::Arc;

use anyhow::{anyhow, Result};
use better_auth::plugins::EmailPasswordPlugin;
use better_auth::{AuthBuilder, AuthConfig, BetterAuth};
use better_auth_diesel_sqlite::DieselSqliteAdapter;

use super::mode::AuthMode;

/// The active authentication layer, stored in [`crate::server::AppState`].
///
/// Wraps the concrete `BetterAuth<DieselSqliteAdapter>` instance in an `Arc`
/// so that `AppState` (and all Axum handler clones) share a single live auth
/// instance without requiring `BetterAuth` itself to be `Clone`.
pub enum AuthLayer {
    None,
    BetterAuthDieselSqlite(Arc<BetterAuth<DieselSqliteAdapter>>),
}

impl std::fmt::Debug for AuthLayer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AuthLayer::None => write!(f, "AuthLayer::None"),
            AuthLayer::BetterAuthDieselSqlite(_) => {
                write!(f, "AuthLayer::BetterAuthDieselSqlite(..)")
            }
        }
    }
}

impl Clone for AuthLayer {
    fn clone(&self) -> Self {
        match self {
            AuthLayer::None => AuthLayer::None,
            AuthLayer::BetterAuthDieselSqlite(arc) => {
                AuthLayer::BetterAuthDieselSqlite(Arc::clone(arc))
            }
        }
    }
}

impl Default for AuthLayer {
    fn default() -> Self {
        AuthLayer::None
    }
}

impl AuthLayer {
    pub async fn build(mode: &AuthMode) -> Result<Self> {
        match mode {
            AuthMode::NoAuth => Ok(AuthLayer::None),
            AuthMode::BetterAuthDieselSqlite => {
                // Capture all env vars synchronously before any async work to avoid
                // race conditions in tests where env vars may be mutated concurrently.
                let secret = std::env::var("AUTH_SECRET").map_err(|_| {
                    anyhow!(
                        "AUTH_SECRET environment variable must be set when \
                         AUTH_MODE=BETTER_AUTH_DIESEL_SQLITE"
                    )
                })?;

                let db_path =
                    std::env::var("AUTH_DB_PATH").unwrap_or_else(|_| "skardi_auth.db".to_string());

                let base_url = std::env::var("AUTH_BASE_URL").unwrap_or_else(|_| {
                    let port = std::env::var("PORT").unwrap_or_else(|_| "8080".to_string());
                    format!("http://localhost:{}", port)
                });

                let db_url = if db_path.starts_with("sqlite://") {
                    db_path.clone()
                } else {
                    format!("sqlite://{}", db_path)
                };

                let adapter = DieselSqliteAdapter::new(&db_url)
                    .await
                    .map_err(|e| anyhow!("Failed to initialize SQLite auth adapter: {}", e))?;

                adapter
                    .run_migrations()
                    .await
                    .map_err(|e| anyhow!("Failed to run auth database migrations: {}", e))?;

                let config = AuthConfig::new(secret)
                    .base_url(base_url)
                    .base_path("/api/auth");

                let auth = AuthBuilder::new(config)
                    .database(adapter)
                    .plugin(EmailPasswordPlugin::new().enable_signup(true))
                    .build()
                    .await
                    .map_err(|e| anyhow!("Failed to initialise BetterAuth: {}", e))?;

                tracing::info!(
                    "BetterAuth initialised (diesel-sqlite at {}). \
                     Sign-up: POST /api/auth/sign-up/email  \
                     Sign-in: POST /api/auth/sign-in/email",
                    db_path
                );

                Ok(AuthLayer::BetterAuthDieselSqlite(Arc::new(auth)))
            }
        }
    }

    pub fn is_enabled(&self) -> bool {
        !matches!(self, AuthLayer::None)
    }

    pub fn as_better_auth(&self) -> Option<&Arc<BetterAuth<DieselSqliteAdapter>>> {
        match self {
            AuthLayer::BetterAuthDieselSqlite(a) => Some(a),
            AuthLayer::None => Option::None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn unique_test_db_path() -> String {
        ":memory:".to_string()
    }

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

    #[tokio::test]
    async fn build_better_auth_missing_secret_errors() {
        unsafe {
            std::env::remove_var("AUTH_SECRET");
            std::env::set_var("AUTH_DB_PATH", unique_test_db_path());
        }
        let result = AuthLayer::build(&AuthMode::BetterAuthDieselSqlite).await;
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
            std::env::set_var("AUTH_DB_PATH", unique_test_db_path());
            std::env::remove_var("AUTH_BASE_URL");
            std::env::remove_var("PORT");
        }

        let layer = AuthLayer::build(&AuthMode::BetterAuthDieselSqlite)
            .await
            .unwrap();
        assert!(layer.is_enabled());
        assert!(layer.as_better_auth().is_some());

        unsafe {
            std::env::remove_var("AUTH_SECRET");
            std::env::remove_var("AUTH_DB_PATH");
        }
    }

    #[tokio::test]
    async fn build_better_auth_respects_auth_base_url() {
        unsafe {
            std::env::set_var("AUTH_SECRET", "test-secret-that-is-at-least-32-characters!");
            std::env::set_var("AUTH_DB_PATH", unique_test_db_path());
            std::env::set_var("AUTH_BASE_URL", "https://example.com");
        }

        let layer = AuthLayer::build(&AuthMode::BetterAuthDieselSqlite)
            .await
            .unwrap();
        let auth = layer.as_better_auth().unwrap();
        assert_eq!(auth.config().base_url, "https://example.com");

        unsafe {
            std::env::remove_var("AUTH_SECRET");
            std::env::remove_var("AUTH_DB_PATH");
            std::env::remove_var("AUTH_BASE_URL");
        }
    }

    #[tokio::test]
    async fn clone_better_auth_shares_arc() {
        unsafe {
            std::env::set_var("AUTH_SECRET", "test-secret-that-is-at-least-32-characters!");
            std::env::set_var("AUTH_DB_PATH", unique_test_db_path());
            std::env::remove_var("AUTH_BASE_URL");
        }

        let layer = AuthLayer::build(&AuthMode::BetterAuthDieselSqlite)
            .await
            .unwrap();
        let cloned = layer.clone();

        assert!(cloned.is_enabled());
        let ptr_a = Arc::as_ptr(layer.as_better_auth().unwrap());
        let ptr_b = Arc::as_ptr(cloned.as_better_auth().unwrap());
        assert_eq!(ptr_a, ptr_b, "clone should share the same Arc");

        unsafe {
            std::env::remove_var("AUTH_SECRET");
            std::env::remove_var("AUTH_DB_PATH");
        }
    }
}
