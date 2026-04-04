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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_is_no_auth() {
        unsafe { std::env::remove_var("AUTH_MODE") };
        assert_eq!(AuthMode::from_env(), AuthMode::NoAuth);
    }

    #[test]
    fn from_env_better_auth_in_memory() {
        unsafe { std::env::set_var("AUTH_MODE", "BETTER_AUTH_IN_MEMORY") };
        assert_eq!(AuthMode::from_env(), AuthMode::BetterAuthInMemory);
        unsafe { std::env::remove_var("AUTH_MODE") };
    }

    #[test]
    fn from_env_unknown_value_defaults_to_no_auth() {
        unsafe { std::env::set_var("AUTH_MODE", "SOMETHING_ELSE") };
        assert_eq!(AuthMode::from_env(), AuthMode::NoAuth);
        unsafe { std::env::remove_var("AUTH_MODE") };
    }

    #[test]
    fn from_env_empty_string_defaults_to_no_auth() {
        unsafe { std::env::set_var("AUTH_MODE", "") };
        assert_eq!(AuthMode::from_env(), AuthMode::NoAuth);
        unsafe { std::env::remove_var("AUTH_MODE") };
    }

    #[test]
    fn is_enabled_no_auth() {
        assert!(!AuthMode::NoAuth.is_enabled());
    }

    #[test]
    fn is_enabled_better_auth() {
        assert!(AuthMode::BetterAuthInMemory.is_enabled());
    }

    #[test]
    fn clone_and_debug() {
        let mode = AuthMode::BetterAuthInMemory;
        let cloned = mode.clone();
        assert_eq!(mode, cloned);
        assert!(format!("{:?}", mode).contains("BetterAuthInMemory"));
    }
}
