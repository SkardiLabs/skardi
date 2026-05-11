//! Bearer-token store backing the `Authorization: Bearer <token>` flow
//! for machine-to-machine and CLI traffic.
//!
//! Better-auth covers browser sessions; this module covers everything
//! that wants a long-lived, scoped credential. A token has the form
//! `skardi_<32-char-base32>`; only the SHA-256 of the full string is
//! persisted, so a leaked database row is not a usable credential.
//!
//! The store is a SQLite file (default `~/.skardi/api_keys.db`,
//! overridable via `SKARDI_API_KEYS_DB`). Schema is created on open and
//! is intentionally tiny — `(id, user_id, name, key_hash, scopes_json,
//! created_at, expires_at, revoked_at)`.
//!
//! Lookups go via `key_hash`, so validation is a single indexed read
//! plus an `expires_at`/`revoked_at` check.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use rand::RngCore;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio_rusqlite::{Connection, rusqlite};
use uuid::Uuid;

const INIT_SCHEMA_SQL: &str = "CREATE TABLE IF NOT EXISTS api_keys (
    id           TEXT PRIMARY KEY,
    user_id      TEXT NOT NULL,
    name         TEXT NOT NULL,
    key_hash     TEXT NOT NULL UNIQUE,
    scopes_json  TEXT NOT NULL,
    created_at   TEXT NOT NULL,
    expires_at   TEXT,
    revoked_at   TEXT
);
CREATE INDEX IF NOT EXISTS idx_api_keys_user ON api_keys (user_id);
CREATE INDEX IF NOT EXISTS idx_api_keys_hash ON api_keys (key_hash);";

/// Public-facing token prefix. Lets `git secret-scanning` and similar
/// tools recognise leaked credentials without false positives.
pub const TOKEN_PREFIX: &str = "skardi_";

/// One row of the api_keys table — used both for listing keys back to
/// the admin UI and for validating an incoming bearer token. The raw
/// token is never stored, so [`ApiKeyRecord`] never carries it.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiKeyRecord {
    pub id: String,
    pub user_id: String,
    pub name: String,
    /// JSON-encoded array of scope strings — see [`super::scope`].
    pub scopes_json: String,
    pub created_at: String,
    pub expires_at: Option<String>,
    pub revoked_at: Option<String>,
}

impl ApiKeyRecord {
    /// True when the key is past its expiry or has been explicitly revoked.
    pub fn is_expired_or_revoked(&self, now: DateTime<Utc>) -> bool {
        if self.revoked_at.is_some() {
            return true;
        }
        match self.expires_at.as_deref() {
            Some(exp) => DateTime::parse_from_rfc3339(exp)
                .map(|t| t.with_timezone(&Utc) <= now)
                .unwrap_or(true),
            None => false,
        }
    }

    pub fn scopes(&self) -> Vec<String> {
        serde_json::from_str(&self.scopes_json).unwrap_or_default()
    }
}

/// Thin handle over the SQLite-backed key store. Cheap to clone (Arc
/// internally), so handlers can hold a reference without lock contention.
#[derive(Clone)]
pub struct ApiKeyStore {
    conn: Arc<Connection>,
}

impl std::fmt::Debug for ApiKeyStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ApiKeyStore").finish_non_exhaustive()
    }
}

impl ApiKeyStore {
    /// Open (or create) the SQLite file and ensure the schema exists.
    /// Idempotent — every server start can call this.
    pub async fn open(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("create dir {}", parent.display()))?;
        }
        let conn = Connection::open(path)
            .await
            .with_context(|| format!("open api_keys db at {}", path.display()))?;
        conn.call(|c| -> std::result::Result<(), rusqlite::Error> {
            c.execute_batch(INIT_SCHEMA_SQL)?;
            Ok(())
        })
        .await
        .context("apply api_keys schema")?;
        Ok(Self {
            conn: Arc::new(conn),
        })
    }

    /// Insert a new key for `user_id`. Returns the raw bearer token —
    /// the only time it is ever surfaced. Caller must hand it to the
    /// user immediately and forget it.
    pub async fn create_key(
        &self,
        user_id: &str,
        name: &str,
        scopes: &[String],
        expires_at: Option<DateTime<Utc>>,
    ) -> Result<(ApiKeyRecord, String)> {
        let raw_token = generate_token();
        let key_hash = hash_token(&raw_token);
        let id = Uuid::new_v4().to_string();
        let scopes_json = serde_json::to_string(scopes).context("encode scopes")?;
        let created_at = Utc::now().to_rfc3339();
        let expires_at_str = expires_at.map(|t| t.to_rfc3339());

        let record = ApiKeyRecord {
            id: id.clone(),
            user_id: user_id.to_string(),
            name: name.to_string(),
            scopes_json: scopes_json.clone(),
            created_at: created_at.clone(),
            expires_at: expires_at_str.clone(),
            revoked_at: None,
        };

        let user_id = user_id.to_string();
        let name = name.to_string();
        self.conn
            .call(move |c| -> std::result::Result<(), rusqlite::Error> {
                c.execute(
                    "INSERT INTO api_keys
                     (id, user_id, name, key_hash, scopes_json, created_at, expires_at, revoked_at)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, NULL)",
                    rusqlite::params![
                        id,
                        user_id,
                        name,
                        key_hash,
                        scopes_json,
                        created_at,
                        expires_at_str,
                    ],
                )?;
                Ok(())
            })
            .await
            .context("insert api_keys row")?;

        Ok((record, raw_token))
    }

    /// Look up a key by its raw bearer token (we hash + index on the hash).
    /// Returns `Ok(None)` for unknown tokens; the caller treats expired or
    /// revoked records as 401 just like a missing token.
    pub async fn lookup_by_token(&self, raw_token: &str) -> Result<Option<ApiKeyRecord>> {
        let key_hash = hash_token(raw_token);
        let record = self
            .conn
            .call(
                move |c| -> std::result::Result<Option<ApiKeyRecord>, rusqlite::Error> {
                    let mut stmt = c.prepare(
                        "SELECT id, user_id, name, scopes_json, created_at, expires_at, revoked_at
                         FROM api_keys WHERE key_hash = ?1 LIMIT 1",
                    )?;
                    let mut rows = stmt.query(rusqlite::params![key_hash])?;
                    if let Some(row) = rows.next()? {
                        Ok(Some(row_to_record(row)?))
                    } else {
                        Ok(None)
                    }
                },
            )
            .await
            .context("lookup api_key by hash")?;
        Ok(record)
    }

    /// All keys belonging to `user_id`. Admin endpoints can pass `None`
    /// to fetch every key in the store.
    pub async fn list_keys(&self, user_id: Option<&str>) -> Result<Vec<ApiKeyRecord>> {
        let user_id = user_id.map(str::to_string);
        let records = self
            .conn
            .call(
                move |c| -> std::result::Result<Vec<ApiKeyRecord>, rusqlite::Error> {
                    let (sql, params): (&str, Vec<String>) = match &user_id {
                        Some(uid) => (
                            "SELECT id, user_id, name, scopes_json, created_at, expires_at, revoked_at
                             FROM api_keys WHERE user_id = ?1
                             ORDER BY created_at DESC",
                            vec![uid.clone()],
                        ),
                        None => (
                            "SELECT id, user_id, name, scopes_json, created_at, expires_at, revoked_at
                             FROM api_keys ORDER BY created_at DESC",
                            vec![],
                        ),
                    };
                    let mut stmt = c.prepare(sql)?;
                    let rows =
                        stmt.query_map(rusqlite::params_from_iter(params), row_to_record)?;
                    let mut out = Vec::new();
                    for r in rows {
                        out.push(r?);
                    }
                    Ok(out)
                },
            )
            .await
            .context("list api_keys")?;
        Ok(records)
    }

    /// Mark `key_id` revoked. Returns true when a row was updated, false
    /// when the id was not found (treated as idempotent — a re-revoke is
    /// not an error from the caller's perspective).
    pub async fn revoke(&self, key_id: &str) -> Result<bool> {
        let key_id = key_id.to_string();
        let now = Utc::now().to_rfc3339();
        let updated = self
            .conn
            .call(move |c| -> std::result::Result<usize, rusqlite::Error> {
                let n = c.execute(
                    "UPDATE api_keys SET revoked_at = ?1
                     WHERE id = ?2 AND revoked_at IS NULL",
                    rusqlite::params![now, key_id],
                )?;
                Ok(n)
            })
            .await
            .context("revoke api_key")?;
        Ok(updated > 0)
    }
}

fn row_to_record(row: &rusqlite::Row<'_>) -> rusqlite::Result<ApiKeyRecord> {
    Ok(ApiKeyRecord {
        id: row.get(0)?,
        user_id: row.get(1)?,
        name: row.get(2)?,
        scopes_json: row.get(3)?,
        created_at: row.get(4)?,
        expires_at: row.get(5)?,
        revoked_at: row.get(6)?,
    })
}

/// Resolve where the api_keys SQLite file should live. Honors
/// `SKARDI_API_KEYS_DB`; otherwise defaults to `$HOME/.skardi/api_keys.db`.
pub fn resolve_default_path() -> Result<PathBuf> {
    if let Ok(p) = std::env::var("SKARDI_API_KEYS_DB") {
        return Ok(PathBuf::from(p));
    }
    let home = dirs::home_dir().context(
        "could not locate a home directory; set SKARDI_API_KEYS_DB to choose the api_keys db path",
    )?;
    Ok(home.join(".skardi").join("api_keys.db"))
}

/// Random 24 bytes → URL-safe base64 (no padding) → `skardi_<token>`.
fn generate_token() -> String {
    use base64::Engine;
    let mut bytes = [0u8; 24];
    rand::thread_rng().fill_bytes(&mut bytes);
    let body = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes);
    format!("{TOKEN_PREFIX}{body}")
}

/// SHA-256 of the full bearer token (prefix included), hex-encoded.
fn hash_token(raw_token: &str) -> String {
    let mut h = Sha256::new();
    h.update(raw_token.as_bytes());
    hex::encode(h.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    async fn fresh_store() -> (ApiKeyStore, TempDir) {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("api_keys.db");
        let store = ApiKeyStore::open(&path).await.unwrap();
        (store, dir)
    }

    #[tokio::test]
    async fn create_then_lookup_round_trip() {
        let (store, _g) = fresh_store().await;
        let scopes = vec!["pipeline:execute:foo".to_string()];
        let (rec, token) = store
            .create_key("user-1", "ci-bot", &scopes, None)
            .await
            .unwrap();
        assert!(token.starts_with(TOKEN_PREFIX));

        let looked_up = store.lookup_by_token(&token).await.unwrap();
        let looked_up = looked_up.expect("token should resolve");
        assert_eq!(looked_up.id, rec.id);
        assert_eq!(looked_up.user_id, "user-1");
        assert_eq!(looked_up.scopes(), scopes);
        assert!(!looked_up.is_expired_or_revoked(Utc::now()));
    }

    #[tokio::test]
    async fn lookup_unknown_token_is_none() {
        let (store, _g) = fresh_store().await;
        let result = store
            .lookup_by_token("skardi_does-not-exist")
            .await
            .unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn revoke_marks_expired_or_revoked() {
        let (store, _g) = fresh_store().await;
        let (rec, token) = store.create_key("user-1", "k", &[], None).await.unwrap();
        assert!(store.revoke(&rec.id).await.unwrap());
        let looked_up = store.lookup_by_token(&token).await.unwrap().unwrap();
        assert!(looked_up.is_expired_or_revoked(Utc::now()));

        // Second revoke is idempotent — returns false but does not error.
        assert!(!store.revoke(&rec.id).await.unwrap());
    }

    #[tokio::test]
    async fn expired_key_is_detected() {
        let (store, _g) = fresh_store().await;
        let past = Utc::now() - chrono::Duration::seconds(60);
        let (_rec, token) = store
            .create_key("user-1", "k", &[], Some(past))
            .await
            .unwrap();
        let looked_up = store.lookup_by_token(&token).await.unwrap().unwrap();
        assert!(looked_up.is_expired_or_revoked(Utc::now()));
    }

    #[tokio::test]
    async fn list_filters_by_user() {
        let (store, _g) = fresh_store().await;
        store.create_key("u1", "a", &[], None).await.unwrap();
        store.create_key("u1", "b", &[], None).await.unwrap();
        store.create_key("u2", "c", &[], None).await.unwrap();

        let u1_keys = store.list_keys(Some("u1")).await.unwrap();
        assert_eq!(u1_keys.len(), 2);

        let all_keys = store.list_keys(None).await.unwrap();
        assert_eq!(all_keys.len(), 3);
    }

    #[tokio::test]
    async fn distinct_tokens_are_distinct() {
        let (store, _g) = fresh_store().await;
        let (_, t1) = store.create_key("u", "a", &[], None).await.unwrap();
        let (_, t2) = store.create_key("u", "b", &[], None).await.unwrap();
        assert_ne!(t1, t2);
    }
}
