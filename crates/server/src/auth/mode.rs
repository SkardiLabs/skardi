/// Controls the authentication mode for the Skardi server.
///
/// Configured via the `AUTH_MODE` environment variable:
/// - `NO_AUTH` (default): all pipeline endpoints are publicly accessible.
/// - `BETTER_AUTH_IN_MEMORY`: uses better-auth backed by a shared in-memory
///   database.  Registration and sign-in are exposed as built-in endpoints;
///   every `/:name/execute` call must carry a valid session token.
#[derive(Debug, Clone, PartialEq)]
pub enum AuthMode {
    /// No authentication — pipeline endpoints are open to all callers.
    NoAuth,
    /// better-auth with an in-memory database (demo / testing only).
    BetterAuthInMemory,
}

impl AuthMode {
    /// Read `AUTH_MODE` from the environment.  Defaults to [`AuthMode::NoAuth`].
    pub fn from_env() -> Self {
        match std::env::var("AUTH_MODE").unwrap_or_default().as_str() {
            "BETTER_AUTH_IN_MEMORY" => AuthMode::BetterAuthInMemory,
            _ => AuthMode::NoAuth,
        }
    }

    pub fn is_enabled(&self) -> bool {
        *self != AuthMode::NoAuth
    }
}
