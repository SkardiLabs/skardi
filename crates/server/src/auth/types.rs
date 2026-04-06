//! Domain types for the auth DataFusion bridge.
//!
//! Each struct owns its data (no lifetime ties to better-auth internals) and
//! knows how to produce an Arrow [`Schema`] and convert a `Vec<Self>` into a
//! [`RecordBatch`].

use std::sync::Arc;

use arrow::array::{BooleanArray, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result as DFResult};

#[derive(Debug, Clone)]
pub struct AuthUserRow {
    pub id: String,
    pub name: Option<String>,
    pub email: Option<String>,
    pub email_verified: bool,
    pub username: Option<String>,
    pub role: Option<String>,
    pub banned: bool,
    pub created_at: String,
    pub updated_at: String,
}

impl AuthUserRow {
    pub fn arrow_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("email", DataType::Utf8, true),
            Field::new("email_verified", DataType::Boolean, false),
            Field::new("username", DataType::Utf8, true),
            Field::new("role", DataType::Utf8, true),
            Field::new("banned", DataType::Boolean, false),
            Field::new("created_at", DataType::Utf8, false),
            Field::new("updated_at", DataType::Utf8, false),
        ]))
    }

    pub fn to_record_batch(rows: &[Self]) -> DFResult<RecordBatch> {
        let schema = Self::arrow_schema();
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(
                    rows.iter().map(|r| Some(r.id.as_str())).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    rows.iter().map(|r| r.name.as_deref()).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    rows.iter().map(|r| r.email.as_deref()).collect::<Vec<_>>(),
                )),
                Arc::new(BooleanArray::from(
                    rows.iter().map(|r| r.email_verified).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    rows.iter()
                        .map(|r| r.username.as_deref())
                        .collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    rows.iter().map(|r| r.role.as_deref()).collect::<Vec<_>>(),
                )),
                Arc::new(BooleanArray::from(
                    rows.iter().map(|r| r.banned).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    rows.iter()
                        .map(|r| Some(r.created_at.as_str()))
                        .collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    rows.iter()
                        .map(|r| Some(r.updated_at.as_str()))
                        .collect::<Vec<_>>(),
                )),
            ],
        )
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
    }
}

impl From<&better_auth::User> for AuthUserRow {
    fn from(u: &better_auth::User) -> Self {
        Self {
            id: u.id.clone(),
            name: u.name.clone(),
            email: u.email.clone(),
            email_verified: u.email_verified,
            username: u.username.clone(),
            role: u.role.clone(),
            banned: u.banned,
            created_at: u.created_at.to_rfc3339(),
            updated_at: u.updated_at.to_rfc3339(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct AuthSessionRow {
    pub id: String,
    pub token: String,
    pub user_id: String,
    pub expires_at: String,
    pub created_at: String,
    pub ip_address: Option<String>,
    pub user_agent: Option<String>,
}

impl AuthSessionRow {
    pub fn arrow_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("token", DataType::Utf8, false),
            Field::new("user_id", DataType::Utf8, false),
            Field::new("expires_at", DataType::Utf8, false),
            Field::new("created_at", DataType::Utf8, false),
            Field::new("ip_address", DataType::Utf8, true),
            Field::new("user_agent", DataType::Utf8, true),
        ]))
    }

    pub fn to_record_batch(rows: &[Self]) -> DFResult<RecordBatch> {
        let schema = Self::arrow_schema();
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(
                    rows.iter().map(|r| Some(r.id.as_str())).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    rows.iter()
                        .map(|r| Some(r.token.as_str()))
                        .collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    rows.iter()
                        .map(|r| Some(r.user_id.as_str()))
                        .collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    rows.iter()
                        .map(|r| Some(r.expires_at.as_str()))
                        .collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    rows.iter()
                        .map(|r| Some(r.created_at.as_str()))
                        .collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    rows.iter()
                        .map(|r| r.ip_address.as_deref())
                        .collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    rows.iter()
                        .map(|r| r.user_agent.as_deref())
                        .collect::<Vec<_>>(),
                )),
            ],
        )
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
    }
}

impl From<&better_auth::Session> for AuthSessionRow {
    fn from(s: &better_auth::Session) -> Self {
        Self {
            id: s.id.clone(),
            token: s.token.clone(),
            user_id: s.user_id.clone(),
            expires_at: s.expires_at.to_rfc3339(),
            created_at: s.created_at.to_rfc3339(),
            ip_address: s.ip_address.clone(),
            user_agent: s.user_agent.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Array;

    // ─── AuthUserRow schema ─────────────────────────────────────────────

    #[test]
    fn user_row_schema_field_count() {
        let schema = AuthUserRow::arrow_schema();
        assert_eq!(schema.fields().len(), 9);
        assert!(schema.field_with_name("id").is_ok());
        assert!(schema.field_with_name("email").is_ok());
        assert!(schema.field_with_name("banned").is_ok());
        assert!(
            schema.field_with_name("image").is_err(),
            "image field should not be present"
        );
    }

    #[test]
    fn user_row_schema_nullability() {
        let schema = AuthUserRow::arrow_schema();
        assert!(!schema.field_with_name("id").unwrap().is_nullable());
        assert!(schema.field_with_name("name").unwrap().is_nullable());
        assert!(schema.field_with_name("email").unwrap().is_nullable());
        assert!(!schema
            .field_with_name("email_verified")
            .unwrap()
            .is_nullable());
        assert!(schema.field_with_name("username").unwrap().is_nullable());
        assert!(schema.field_with_name("role").unwrap().is_nullable());
        assert!(!schema.field_with_name("banned").unwrap().is_nullable());
        assert!(!schema.field_with_name("created_at").unwrap().is_nullable());
        assert!(!schema.field_with_name("updated_at").unwrap().is_nullable());
    }

    #[test]
    fn user_row_schema_data_types() {
        let schema = AuthUserRow::arrow_schema();
        assert_eq!(
            *schema.field_with_name("id").unwrap().data_type(),
            DataType::Utf8
        );
        assert_eq!(
            *schema
                .field_with_name("email_verified")
                .unwrap()
                .data_type(),
            DataType::Boolean
        );
        assert_eq!(
            *schema.field_with_name("banned").unwrap().data_type(),
            DataType::Boolean
        );
    }

    // ─── AuthSessionRow schema ──────────────────────────────────────────

    #[test]
    fn session_row_schema_field_count() {
        let schema = AuthSessionRow::arrow_schema();
        assert_eq!(schema.fields().len(), 7);
        assert!(schema.field_with_name("token").is_ok());
        assert!(schema.field_with_name("user_id").is_ok());
    }

    #[test]
    fn session_row_schema_nullability() {
        let schema = AuthSessionRow::arrow_schema();
        assert!(!schema.field_with_name("id").unwrap().is_nullable());
        assert!(!schema.field_with_name("token").unwrap().is_nullable());
        assert!(!schema.field_with_name("user_id").unwrap().is_nullable());
        assert!(!schema.field_with_name("expires_at").unwrap().is_nullable());
        assert!(!schema.field_with_name("created_at").unwrap().is_nullable());
        assert!(schema.field_with_name("ip_address").unwrap().is_nullable());
        assert!(schema.field_with_name("user_agent").unwrap().is_nullable());
    }

    // ─── Empty batches ──────────────────────────────────────────────────

    #[test]
    fn user_row_empty_batch() {
        let batch = AuthUserRow::to_record_batch(&[]).unwrap();
        assert_eq!(batch.num_rows(), 0);
        assert_eq!(batch.num_columns(), 9);
        assert_eq!(*batch.schema(), *AuthUserRow::arrow_schema());
    }

    #[test]
    fn session_row_empty_batch() {
        let batch = AuthSessionRow::to_record_batch(&[]).unwrap();
        assert_eq!(batch.num_rows(), 0);
        assert_eq!(batch.num_columns(), 7);
        assert_eq!(*batch.schema(), *AuthSessionRow::arrow_schema());
    }

    // ─── Roundtrip (all-populated vs all-null optional fields) ──────────

    #[test]
    fn user_row_roundtrip() {
        let rows = vec![
            AuthUserRow {
                id: "u1".into(),
                name: Some("Alice".into()),
                email: Some("alice@example.com".into()),
                email_verified: true,
                username: Some("alice".into()),
                role: Some("admin".into()),
                banned: false,
                created_at: "2025-01-01T00:00:00+00:00".into(),
                updated_at: "2025-01-02T00:00:00+00:00".into(),
            },
            AuthUserRow {
                id: "u2".into(),
                name: None,
                email: None,
                email_verified: false,
                username: None,
                role: None,
                banned: true,
                created_at: "2025-06-01T00:00:00+00:00".into(),
                updated_at: "2025-06-01T00:00:00+00:00".into(),
            },
        ];

        let batch = AuthUserRow::to_record_batch(&rows).unwrap();
        assert_eq!(batch.num_rows(), 2);

        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(ids.value(0), "u1");
        assert_eq!(ids.value(1), "u2");

        let names = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(names.value(0), "Alice");
        assert!(names.is_null(1));

        let emails = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(emails.value(0), "alice@example.com");
        assert!(emails.is_null(1));

        let verified = batch
            .column(3)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert!(verified.value(0));
        assert!(!verified.value(1));

        let usernames = batch
            .column(4)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(usernames.value(0), "alice");
        assert!(usernames.is_null(1));

        let roles = batch
            .column(5)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(roles.value(0), "admin");
        assert!(roles.is_null(1));

        let banned = batch
            .column(6)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert!(!banned.value(0));
        assert!(banned.value(1));

        let created = batch
            .column(7)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(created.value(0), "2025-01-01T00:00:00+00:00");
    }

    #[test]
    fn session_row_roundtrip() {
        let rows = vec![AuthSessionRow {
            id: "s1".into(),
            token: "tok_abc".into(),
            user_id: "u1".into(),
            expires_at: "2025-12-31T23:59:59+00:00".into(),
            created_at: "2025-01-01T00:00:00+00:00".into(),
            ip_address: Some("127.0.0.1".into()),
            user_agent: None,
        }];

        let batch = AuthSessionRow::to_record_batch(&rows).unwrap();
        assert_eq!(batch.num_rows(), 1);

        let tokens = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(tokens.value(0), "tok_abc");

        let ips = batch
            .column(5)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(ips.value(0), "127.0.0.1");

        let agents = batch
            .column(6)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!(agents.is_null(0));
    }

    #[test]
    fn session_row_both_optional_fields_set() {
        let rows = vec![AuthSessionRow {
            id: "s2".into(),
            token: "tok_xyz".into(),
            user_id: "u2".into(),
            expires_at: "2025-12-31T00:00:00+00:00".into(),
            created_at: "2025-06-01T00:00:00+00:00".into(),
            ip_address: Some("10.0.0.1".into()),
            user_agent: Some("Mozilla/5.0".into()),
        }];

        let batch = AuthSessionRow::to_record_batch(&rows).unwrap();
        let ips = batch
            .column(5)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(ips.value(0), "10.0.0.1");

        let agents = batch
            .column(6)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(agents.value(0), "Mozilla/5.0");
    }

    #[test]
    fn session_row_both_optional_fields_null() {
        let rows = vec![AuthSessionRow {
            id: "s3".into(),
            token: "tok_null".into(),
            user_id: "u3".into(),
            expires_at: "2026-01-01T00:00:00+00:00".into(),
            created_at: "2025-01-01T00:00:00+00:00".into(),
            ip_address: None,
            user_agent: None,
        }];

        let batch = AuthSessionRow::to_record_batch(&rows).unwrap();
        let ips = batch
            .column(5)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!(ips.is_null(0));

        let agents = batch
            .column(6)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!(agents.is_null(0));
    }

    // ─── Multiple rows batch ────────────────────────────────────────────

    #[test]
    fn session_row_multiple_rows() {
        let rows = vec![
            AuthSessionRow {
                id: "s1".into(),
                token: "tok1".into(),
                user_id: "u1".into(),
                expires_at: "2026-01-01T00:00:00+00:00".into(),
                created_at: "2025-01-01T00:00:00+00:00".into(),
                ip_address: Some("1.1.1.1".into()),
                user_agent: Some("Chrome".into()),
            },
            AuthSessionRow {
                id: "s2".into(),
                token: "tok2".into(),
                user_id: "u1".into(),
                expires_at: "2026-01-01T00:00:00+00:00".into(),
                created_at: "2025-02-01T00:00:00+00:00".into(),
                ip_address: None,
                user_agent: None,
            },
            AuthSessionRow {
                id: "s3".into(),
                token: "tok3".into(),
                user_id: "u2".into(),
                expires_at: "2026-06-01T00:00:00+00:00".into(),
                created_at: "2025-03-01T00:00:00+00:00".into(),
                ip_address: Some("2.2.2.2".into()),
                user_agent: None,
            },
        ];

        let batch = AuthSessionRow::to_record_batch(&rows).unwrap();
        assert_eq!(batch.num_rows(), 3);

        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(ids.value(0), "s1");
        assert_eq!(ids.value(1), "s2");
        assert_eq!(ids.value(2), "s3");
    }

    // ─── From conversions (requires better-auth types) ──────────────────

    fn sample_better_auth_user(id: &str) -> better_auth::User {
        better_auth::User {
            id: id.to_string(),
            name: Some("Test User".to_string()),
            email: Some("test@example.com".to_string()),
            email_verified: true,
            image: Some("https://example.com/img.png".to_string()),
            username: Some("testuser".to_string()),
            display_username: None,
            two_factor_enabled: false,
            role: Some("editor".to_string()),
            banned: false,
            ban_reason: None,
            ban_expires: None,
            metadata: serde_json::Value::Null,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        }
    }

    fn sample_better_auth_session(id: &str) -> better_auth::Session {
        let now = chrono::Utc::now();
        better_auth::Session {
            id: id.to_string(),
            token: "tok-session-1".to_string(),
            user_id: "uid-1".to_string(),
            expires_at: now + chrono::Duration::hours(24),
            created_at: now,
            updated_at: now,
            ip_address: Some("192.168.1.1".to_string()),
            user_agent: Some("TestAgent/1.0".to_string()),
            impersonated_by: None,
            active_organization_id: None,
            active: true,
        }
    }

    #[test]
    fn from_better_auth_user() {
        let ba_user = sample_better_auth_user("uid-1");
        let now_date = chrono::Utc::now().format("%Y-%m-%d").to_string();

        let row = AuthUserRow::from(&ba_user);
        assert_eq!(row.id, "uid-1");
        assert_eq!(row.name.as_deref(), Some("Test User"));
        assert_eq!(row.email.as_deref(), Some("test@example.com"));
        assert!(row.email_verified);
        assert_eq!(row.username.as_deref(), Some("testuser"));
        assert_eq!(row.role.as_deref(), Some("editor"));
        assert!(!row.banned);
        assert!(row.created_at.contains(&now_date));
    }

    #[test]
    fn from_better_auth_user_minimal() {
        let mut ba_user = sample_better_auth_user("uid-2");
        ba_user.name = None;
        ba_user.email = None;
        ba_user.email_verified = false;
        ba_user.image = None;
        ba_user.username = None;
        ba_user.role = None;
        ba_user.banned = true;

        let row = AuthUserRow::from(&ba_user);
        assert_eq!(row.id, "uid-2");
        assert!(row.name.is_none());
        assert!(row.email.is_none());
        assert!(!row.email_verified);
        assert!(row.username.is_none());
        assert!(row.role.is_none());
        assert!(row.banned);
    }

    #[test]
    fn from_better_auth_user_omits_image() {
        let ba_user = sample_better_auth_user("uid-3");
        assert!(ba_user.image.is_some(), "precondition: source has image");
        let row = AuthUserRow::from(&ba_user);
        let batch = AuthUserRow::to_record_batch(&[row]).unwrap();
        assert!(
            batch.schema().field_with_name("image").is_err(),
            "image column must not appear in output"
        );
    }

    #[test]
    fn from_better_auth_session() {
        let ba_session = sample_better_auth_session("sid-1");

        let row = AuthSessionRow::from(&ba_session);
        assert_eq!(row.id, "sid-1");
        assert_eq!(row.token, "tok-session-1");
        assert_eq!(row.user_id, "uid-1");
        assert_eq!(row.ip_address.as_deref(), Some("192.168.1.1"));
        assert_eq!(row.user_agent.as_deref(), Some("TestAgent/1.0"));
        assert_eq!(row.expires_at, ba_session.expires_at.to_rfc3339());
        assert_eq!(row.created_at, ba_session.created_at.to_rfc3339());
    }

    #[test]
    fn from_better_auth_session_no_optionals() {
        let mut ba_session = sample_better_auth_session("sid-2");
        ba_session.ip_address = None;
        ba_session.user_agent = None;

        let row = AuthSessionRow::from(&ba_session);
        assert!(row.ip_address.is_none());
        assert!(row.user_agent.is_none());
    }

    // ─── Clone / Debug ──────────────────────────────────────────────────

    #[test]
    fn user_row_clone_and_debug() {
        let row = AuthUserRow {
            id: "u1".into(),
            name: Some("A".into()),
            email: None,
            email_verified: false,
            username: None,
            role: None,
            banned: false,
            created_at: "t".into(),
            updated_at: "t".into(),
        };
        let cloned = row.clone();
        assert_eq!(cloned.id, "u1");
        let dbg = format!("{:?}", row);
        assert!(dbg.contains("AuthUserRow"));
    }

    #[test]
    fn session_row_clone_and_debug() {
        let row = AuthSessionRow {
            id: "s1".into(),
            token: "t".into(),
            user_id: "u1".into(),
            expires_at: "t".into(),
            created_at: "t".into(),
            ip_address: None,
            user_agent: None,
        };
        let cloned = row.clone();
        assert_eq!(cloned.id, "s1");
        let dbg = format!("{:?}", row);
        assert!(dbg.contains("AuthSessionRow"));
    }

    // ─── Schema consistency ─────────────────────────────────────────────

    #[test]
    fn batch_schema_equals_arrow_schema_for_users() {
        let rows = vec![AuthUserRow {
            id: "x".into(),
            name: None,
            email: None,
            email_verified: false,
            username: None,
            role: None,
            banned: false,
            created_at: "t".into(),
            updated_at: "t".into(),
        }];
        let batch = AuthUserRow::to_record_batch(&rows).unwrap();
        assert_eq!(*batch.schema(), *AuthUserRow::arrow_schema());
    }

    #[test]
    fn batch_schema_equals_arrow_schema_for_sessions() {
        let rows = vec![AuthSessionRow {
            id: "x".into(),
            token: "t".into(),
            user_id: "u".into(),
            expires_at: "e".into(),
            created_at: "c".into(),
            ip_address: None,
            user_agent: None,
        }];
        let batch = AuthSessionRow::to_record_batch(&rows).unwrap();
        assert_eq!(*batch.schema(), *AuthSessionRow::arrow_schema());
    }
}
