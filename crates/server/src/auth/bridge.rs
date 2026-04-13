//! Arrow / DataFusion bridge for better-auth's SQLite database.
//!
//! Registers two virtual tables in the shared `SessionContext` under the
//! `auth` schema:
//!
//! * `auth.users`    — mirrors the `User` store (live read on every scan)
//! * `auth.sessions` — mirrors the `Session` store (live read on every scan)
//!
//! Every `scan()` call queries the underlying SQLite database through the
//! adapter's `UserOps`/`SessionOps` trait implementations, so queries always
//! reflect the latest state without any manual refresh step.

use std::any::Any;
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use better_auth::types_mod::ListUsersParams;
use better_auth::{BetterAuth, SessionOps, UserOps};
use better_auth_diesel_sqlite::DieselSqliteAdapter;
use datafusion::catalog::{MemorySchemaProvider, SchemaProvider, Session as DFSession};
use datafusion::datasource::memory::MemTable;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;

use super::types::{AuthSessionRow, AuthUserRow};

// ---------------------------------------------------------------------------
// auth.users
// ---------------------------------------------------------------------------

/// DataFusion `TableProvider` that reads the live `auth.users` snapshot.
pub struct AuthUsersTable {
    auth: Arc<BetterAuth<DieselSqliteAdapter>>,
    schema: SchemaRef,
}

impl fmt::Debug for AuthUsersTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AuthUsersTable")
            .field("schema", &self.schema)
            .finish_non_exhaustive()
    }
}

impl AuthUsersTable {
    pub fn new(auth: Arc<BetterAuth<DieselSqliteAdapter>>) -> Self {
        Self {
            auth,
            schema: AuthUserRow::arrow_schema(),
        }
    }

    async fn build_batch(&self) -> DFResult<RecordBatch> {
        let db = self.auth.database();
        let (users, _) = db
            .list_users(ListUsersParams::default())
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let rows: Vec<AuthUserRow> = users.iter().map(AuthUserRow::from).collect();
        AuthUserRow::to_record_batch(&rows)
    }
}

#[async_trait]
impl TableProvider for AuthUsersTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn DFSession,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let batch = self.build_batch().await?;
        let mem_table = MemTable::try_new(self.schema.clone(), vec![vec![batch]])?;
        mem_table.scan(state, projection, filters, limit).await
    }
}

// ---------------------------------------------------------------------------
// auth.sessions
// ---------------------------------------------------------------------------

/// DataFusion `TableProvider` that reads the live `auth.sessions` snapshot.
///
/// Because `SessionOps` has no "list all sessions" method, we collect every
/// user's sessions via `get_user_sessions`.
pub struct AuthSessionsTable {
    auth: Arc<BetterAuth<DieselSqliteAdapter>>,
    schema: SchemaRef,
}

impl fmt::Debug for AuthSessionsTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AuthSessionsTable")
            .field("schema", &self.schema)
            .finish_non_exhaustive()
    }
}

impl AuthSessionsTable {
    pub fn new(auth: Arc<BetterAuth<DieselSqliteAdapter>>) -> Self {
        Self {
            auth,
            schema: AuthSessionRow::arrow_schema(),
        }
    }

    async fn build_batch(&self) -> DFResult<RecordBatch> {
        let db = self.auth.database();

        let (users, _) = db
            .list_users(ListUsersParams::default())
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let mut all_sessions = Vec::new();
        for user in &users {
            let sessions = db
                .get_user_sessions(&user.id)
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            all_sessions.extend(sessions);
        }

        let rows: Vec<AuthSessionRow> = all_sessions.iter().map(AuthSessionRow::from).collect();
        AuthSessionRow::to_record_batch(&rows)
    }
}

#[async_trait]
impl TableProvider for AuthSessionsTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn DFSession,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let batch = self.build_batch().await?;
        let mem_table = MemTable::try_new(self.schema.clone(), vec![vec![batch]])?;
        mem_table.scan(state, projection, filters, limit).await
    }
}

// ---------------------------------------------------------------------------
// Registration helper
// ---------------------------------------------------------------------------

/// Register `auth.users` and `auth.sessions` virtual tables into the given
/// DataFusion `SessionContext` under a dedicated `auth` schema.
pub fn register_auth_tables(
    ctx: &mut SessionContext,
    auth: Arc<BetterAuth<DieselSqliteAdapter>>,
) -> anyhow::Result<()> {
    let schema = Arc::new(MemorySchemaProvider::new());

    schema
        .register_table(
            "users".into(),
            Arc::new(AuthUsersTable::new(Arc::clone(&auth))),
        )
        .map_err(|e| anyhow::anyhow!("Failed to register auth.users table: {}", e))?;

    schema
        .register_table(
            "sessions".into(),
            Arc::new(AuthSessionsTable::new(Arc::clone(&auth))),
        )
        .map_err(|e| anyhow::anyhow!("Failed to register auth.sessions table: {}", e))?;

    let catalog = ctx
        .catalog("datafusion")
        .ok_or_else(|| anyhow::anyhow!("Default catalog 'datafusion' not found"))?;

    if catalog.schema("auth").is_some() {
        return Err(anyhow::anyhow!("Auth schema 'auth' is already registered"));
    }

    catalog
        .register_schema("auth", schema)
        .map_err(|e| anyhow::anyhow!("Failed to register auth schema: {}", e))?;

    tracing::info!("Registered DataFusion tables: auth.users, auth.sessions");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use better_auth::plugins::EmailPasswordPlugin;
    use better_auth::types_mod::CreateSession;
    use better_auth::{AuthBuilder, AuthConfig, SessionOps, UserOps};

    async fn test_auth() -> Arc<BetterAuth<DieselSqliteAdapter>> {
        let adapter = DieselSqliteAdapter::in_memory().await.unwrap();
        adapter.run_migrations().await.unwrap();
        let config = AuthConfig::new("test-secret-that-is-at-least-32-characters!")
            .base_url("http://localhost:8080")
            .base_path("/api/auth");

        let auth = AuthBuilder::new(config)
            .database(adapter)
            .plugin(EmailPasswordPlugin::new().enable_signup(true))
            .build()
            .await
            .unwrap();
        Arc::new(auth)
    }

    // ─── AuthUsersTable ─────────────────────────────────────────────────

    #[tokio::test]
    async fn users_table_schema_matches_type() {
        let auth = test_auth().await;
        let table = AuthUsersTable::new(Arc::clone(&auth));
        assert_eq!(
            table.schema(),
            super::super::types::AuthUserRow::arrow_schema()
        );
        assert_eq!(table.table_type(), TableType::Base);
    }

    #[tokio::test]
    async fn users_table_empty_scan() {
        let auth = test_auth().await;
        let table = AuthUsersTable::new(Arc::clone(&auth));
        let batch = table.build_batch().await.unwrap();
        assert_eq!(batch.num_rows(), 0);
        assert_eq!(batch.num_columns(), 9);
    }

    #[tokio::test]
    async fn users_table_scan_after_create() {
        let auth = test_auth().await;
        auth.database()
            .create_user(better_auth::types_mod::CreateUser {
                name: Some("Alice".into()),
                email: Some("alice@example.com".into()),
                password: Some("securepass123".into()),
                ..Default::default()
            })
            .await
            .unwrap();

        let table = AuthUsersTable::new(Arc::clone(&auth));
        let batch = table.build_batch().await.unwrap();
        assert_eq!(batch.num_rows(), 1);

        let emails = batch
            .column(2)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert_eq!(emails.value(0), "alice@example.com");
    }

    #[tokio::test]
    async fn users_table_debug_format() {
        let auth = test_auth().await;
        let table = AuthUsersTable::new(auth);
        let dbg = format!("{:?}", table);
        assert!(dbg.contains("AuthUsersTable"));
    }

    // ─── AuthSessionsTable ──────────────────────────────────────────────

    #[tokio::test]
    async fn sessions_table_schema_matches_type() {
        let auth = test_auth().await;
        let table = AuthSessionsTable::new(Arc::clone(&auth));
        assert_eq!(
            table.schema(),
            super::super::types::AuthSessionRow::arrow_schema()
        );
        assert_eq!(table.table_type(), TableType::Base);
    }

    #[tokio::test]
    async fn sessions_table_empty_scan() {
        let auth = test_auth().await;
        let table = AuthSessionsTable::new(Arc::clone(&auth));
        let batch = table.build_batch().await.unwrap();
        assert_eq!(batch.num_rows(), 0);
        assert_eq!(batch.num_columns(), 7);
    }

    #[tokio::test]
    async fn sessions_table_scan_after_create_session() {
        let auth = test_auth().await;
        let user = auth
            .database()
            .create_user(better_auth::types_mod::CreateUser {
                name: Some("Bob".into()),
                email: Some("bob@example.com".into()),
                password: Some("password123".into()),
                ..Default::default()
            })
            .await
            .unwrap();

        auth.database()
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

        let table = AuthSessionsTable::new(Arc::clone(&auth));
        let batch = table.build_batch().await.unwrap();
        assert_eq!(batch.num_rows(), 1);

        let user_ids = batch
            .column(2)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert_eq!(user_ids.value(0), user.id);
    }

    #[tokio::test]
    async fn sessions_table_multiple_users_multiple_sessions() {
        let auth = test_auth().await;

        let u1 = auth
            .database()
            .create_user(better_auth::types_mod::CreateUser {
                name: Some("User1".into()),
                email: Some("u1@test.com".into()),
                password: Some("password123".into()),
                ..Default::default()
            })
            .await
            .unwrap();

        let u2 = auth
            .database()
            .create_user(better_auth::types_mod::CreateUser {
                name: Some("User2".into()),
                email: Some("u2@test.com".into()),
                password: Some("password123".into()),
                ..Default::default()
            })
            .await
            .unwrap();

        let expires = chrono::Utc::now() + chrono::Duration::hours(1);
        for uid in [&u1.id, &u1.id, &u2.id] {
            auth.database()
                .create_session(CreateSession {
                    user_id: uid.clone(),
                    expires_at: expires,
                    ip_address: None,
                    user_agent: None,
                    impersonated_by: None,
                    active_organization_id: None,
                })
                .await
                .unwrap();
        }

        let table = AuthSessionsTable::new(Arc::clone(&auth));
        let batch = table.build_batch().await.unwrap();
        assert_eq!(batch.num_rows(), 3);
    }

    #[tokio::test]
    async fn sessions_table_debug_format() {
        let auth = test_auth().await;
        let table = AuthSessionsTable::new(auth);
        let dbg = format!("{:?}", table);
        assert!(dbg.contains("AuthSessionsTable"));
    }

    // ─── register_auth_tables ───────────────────────────────────────────

    #[tokio::test]
    async fn register_tables_creates_both() {
        let auth = test_auth().await;
        let mut ctx = SessionContext::new();

        register_auth_tables(&mut ctx, Arc::clone(&auth)).unwrap();

        assert!(ctx.table("auth.users").await.is_ok());
        assert!(ctx.table("auth.sessions").await.is_ok());
    }

    #[tokio::test]
    async fn register_tables_duplicate_errors() {
        let auth = test_auth().await;
        let mut ctx = SessionContext::new();
        register_auth_tables(&mut ctx, Arc::clone(&auth)).unwrap();

        let result = register_auth_tables(&mut ctx, Arc::clone(&auth));
        assert!(result.is_err(), "re-registering should fail");
    }

    #[tokio::test]
    async fn query_registered_auth_users() {
        let auth = test_auth().await;
        auth.database()
            .create_user(better_auth::types_mod::CreateUser {
                name: Some("QueryUser".into()),
                email: Some("query@test.com".into()),
                password: Some("password123".into()),
                ..Default::default()
            })
            .await
            .unwrap();

        let mut ctx = SessionContext::new();
        register_auth_tables(&mut ctx, Arc::clone(&auth)).unwrap();

        let df = ctx.sql("SELECT id, email FROM auth.users").await.unwrap();
        let batches = df.collect().await.unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);

        let emails = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert_eq!(emails.value(0), "query@test.com");
    }

    #[tokio::test]
    async fn query_registered_auth_sessions() {
        let auth = test_auth().await;
        let user = auth
            .database()
            .create_user(better_auth::types_mod::CreateUser {
                name: Some("SessUser".into()),
                email: Some("sess@test.com".into()),
                password: Some("password123".into()),
                ..Default::default()
            })
            .await
            .unwrap();

        auth.database()
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

        let mut ctx = SessionContext::new();
        register_auth_tables(&mut ctx, Arc::clone(&auth)).unwrap();

        let df = ctx
            .sql("SELECT user_id, token FROM auth.sessions")
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);

        let user_ids = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert_eq!(user_ids.value(0), user.id);
    }
}
