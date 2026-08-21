pub mod bridge;
pub mod layer;
pub mod mode;
pub mod routes;
pub mod types;

#[cfg(test)]
pub(crate) mod test_env {
    use tokio::sync::{Mutex, MutexGuard};

    /// Serializes tests that mutate `AUTH_SECRET` / `AUTH_DB_PATH` /
    /// `AUTH_BASE_URL`.
    ///
    /// Environment variables are process-global, and `layer.rs` and
    /// `routes.rs` tests share one test binary that libtest runs in parallel.
    /// `build_better_auth_missing_secret_errors` removes `AUTH_SECRET` and
    /// asserts the build fails; every other auth test sets it. Without
    /// serialization those two race, and the removal test intermittently sees
    /// a secret another test just set and fails with "expected Err when
    /// AUTH_SECRET is unset".
    ///
    /// A `tokio::sync::Mutex` rather than `std::sync::Mutex` because the
    /// guard must be held across `AuthLayer::build(..).await` — locking only
    /// around the mutation would still let another test set the variable
    /// between the mutation and the build that reads it.
    static ENV_LOCK: Mutex<()> = Mutex::const_new(());

    /// Hold the returned guard for as long as the process environment must
    /// stay as this test left it — i.e. through the `build` that reads it.
    pub(crate) async fn lock() -> MutexGuard<'static, ()> {
        ENV_LOCK.lock().await
    }
}
