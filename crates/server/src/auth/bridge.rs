//! Arrow / DataFusion bridge for better-auth's in-memory database.
//!
//! Registers two virtual tables in the shared `SessionContext`:
//!
//! * `auth_users`    — mirrors the [`User`] store (live read on every scan)
//! * `auth_sessions` — mirrors the [`Session`] store (live read on every scan)
//!
//! Because the in-memory adapter holds its data behind `Arc<Mutex<…>>`, every
//! `scan()` call grabs the current snapshot, so queries always reflect the
//! latest registrations and sign-ins without any manual refresh step.

use std::any::Any;
use std::fmt;
use std::sync::Arc;

use arrow::array::{BooleanArray, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use better_auth::types_mod::ListUsersParams;
use better_auth::{AuthSession, BetterAuth, MemoryDatabaseAdapter, SessionOps, UserOps};
use datafusion::catalog::Session as DFSession;
use datafusion::datasource::memory::MemTable;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;

// ---------------------------------------------------------------------------
// auth_users
// ---------------------------------------------------------------------------

/// DataFusion `TableProvider` that reads the live `auth_users` snapshot.
pub struct AuthUsersTable {
    auth: Arc<BetterAuth<MemoryDatabaseAdapter>>,
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
    pub fn new(auth: Arc<BetterAuth<MemoryDatabaseAdapter>>) -> Self {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("email", DataType::Utf8, true),
            Field::new("email_verified", DataType::Boolean, false),
            Field::new("image", DataType::Utf8, true),
            Field::new("username", DataType::Utf8, true),
            Field::new("role", DataType::Utf8, true),
            Field::new("banned", DataType::Boolean, false),
            Field::new("created_at", DataType::Utf8, false),
            Field::new("updated_at", DataType::Utf8, false),
        ]));
        Self { auth, schema }
    }

    async fn build_batch(&self) -> DFResult<RecordBatch> {
        let db = self.auth.database();
        let (users, _) = db
            .list_users(ListUsersParams::default())
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let ids: Vec<Option<&str>> = users.iter().map(|u| Some(u.id.as_str())).collect();
        let names: Vec<Option<&str>> = users.iter().map(|u| u.name.as_deref()).collect();
        let emails: Vec<Option<&str>> = users.iter().map(|u| u.email.as_deref()).collect();
        let email_verified: Vec<bool> = users.iter().map(|u| u.email_verified).collect();
        let images: Vec<Option<&str>> = users.iter().map(|u| u.image.as_deref()).collect();
        let usernames: Vec<Option<&str>> = users.iter().map(|u| u.username.as_deref()).collect();
        let roles: Vec<Option<&str>> = users.iter().map(|u| u.role.as_deref()).collect();
        let banned: Vec<bool> = users.iter().map(|u| u.banned).collect();
        let created_at: Vec<Option<&str>> = users.iter().map(|_| Option::<&str>::None).collect(); // placeholder strings
        let updated_at: Vec<Option<&str>> = users.iter().map(|_| Option::<&str>::None).collect();

        // Format timestamps as RFC3339 strings.
        let created_at_strings: Vec<String> =
            users.iter().map(|u| u.created_at.to_rfc3339()).collect();
        let updated_at_strings: Vec<String> =
            users.iter().map(|u| u.updated_at.to_rfc3339()).collect();
        let created_at_refs: Vec<Option<&str>> = created_at_strings
            .iter()
            .map(|s| Some(s.as_str()))
            .collect();
        let updated_at_refs: Vec<Option<&str>> = updated_at_strings
            .iter()
            .map(|s| Some(s.as_str()))
            .collect();

        let _ = created_at;
        let _ = updated_at;

        RecordBatch::try_new(
            self.schema.clone(),
            vec![
                Arc::new(StringArray::from(ids)),
                Arc::new(StringArray::from(names)),
                Arc::new(StringArray::from(emails)),
                Arc::new(BooleanArray::from(email_verified)),
                Arc::new(StringArray::from(images)),
                Arc::new(StringArray::from(usernames)),
                Arc::new(StringArray::from(roles)),
                Arc::new(BooleanArray::from(banned)),
                Arc::new(StringArray::from(created_at_refs)),
                Arc::new(StringArray::from(updated_at_refs)),
            ],
        )
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
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
// auth_sessions
// ---------------------------------------------------------------------------

/// DataFusion `TableProvider` that reads the live `auth_sessions` snapshot.
///
/// Because `SessionOps` has no "list all sessions" method, we collect every
/// user's sessions via `get_user_sessions` — perfectly acceptable for a demo
/// backed by an in-memory store.
pub struct AuthSessionsTable {
    auth: Arc<BetterAuth<MemoryDatabaseAdapter>>,
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
    pub fn new(auth: Arc<BetterAuth<MemoryDatabaseAdapter>>) -> Self {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("token", DataType::Utf8, false),
            Field::new("user_id", DataType::Utf8, false),
            Field::new("expires_at", DataType::Utf8, false),
            Field::new("created_at", DataType::Utf8, false),
            Field::new("ip_address", DataType::Utf8, true),
            Field::new("user_agent", DataType::Utf8, true),
        ]));
        Self { auth, schema }
    }

    async fn build_batch(&self) -> DFResult<RecordBatch> {
        let db = self.auth.database();

        // Collect all sessions by iterating over every user.
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

        let ids: Vec<Option<&str>> = all_sessions.iter().map(|s| Some(s.id.as_str())).collect();
        let tokens: Vec<Option<&str>> = all_sessions
            .iter()
            .map(|s| Some(s.token.as_str()))
            .collect();
        let user_ids: Vec<Option<&str>> = all_sessions
            .iter()
            .map(|s| Some(s.user_id.as_str()))
            .collect();

        let expires_at_strings: Vec<String> = all_sessions
            .iter()
            .map(|s| s.expires_at().to_rfc3339())
            .collect();
        let created_at_strings: Vec<String> = all_sessions
            .iter()
            .map(|s| s.created_at.to_rfc3339())
            .collect();

        let expires_at_refs: Vec<Option<&str>> = expires_at_strings
            .iter()
            .map(|s| Some(s.as_str()))
            .collect();
        let created_at_refs: Vec<Option<&str>> = created_at_strings
            .iter()
            .map(|s| Some(s.as_str()))
            .collect();

        let ip_addresses: Vec<Option<&str>> = all_sessions
            .iter()
            .map(|s| s.ip_address.as_deref())
            .collect();
        let user_agents: Vec<Option<&str>> = all_sessions
            .iter()
            .map(|s| s.user_agent.as_deref())
            .collect();

        RecordBatch::try_new(
            self.schema.clone(),
            vec![
                Arc::new(StringArray::from(ids)),
                Arc::new(StringArray::from(tokens)),
                Arc::new(StringArray::from(user_ids)),
                Arc::new(StringArray::from(expires_at_refs)),
                Arc::new(StringArray::from(created_at_refs)),
                Arc::new(StringArray::from(ip_addresses)),
                Arc::new(StringArray::from(user_agents)),
            ],
        )
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
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

/// Register `auth_users` and `auth_sessions` virtual tables into the given
/// DataFusion `SessionContext`.
pub fn register_auth_tables(
    ctx: &mut SessionContext,
    auth: Arc<BetterAuth<MemoryDatabaseAdapter>>,
) -> anyhow::Result<()> {
    ctx.register_table(
        "auth_users",
        Arc::new(AuthUsersTable::new(Arc::clone(&auth))),
    )
    .map_err(|e| anyhow::anyhow!("Failed to register auth_users table: {}", e))?;

    ctx.register_table(
        "auth_sessions",
        Arc::new(AuthSessionsTable::new(Arc::clone(&auth))),
    )
    .map_err(|e| anyhow::anyhow!("Failed to register auth_sessions table: {}", e))?;

    tracing::info!("Registered DataFusion tables: auth_users, auth_sessions");
    Ok(())
}
