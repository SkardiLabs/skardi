//! Shared helpers for MySQL wire-protocol providers (MySQL, SeekDB).
//!
//! The vanilla MySQL provider and the SeekDB provider both drive
//! `datafusion_table_providers::sql::db_connection_pool::mysqlpool::MySQLConnectionPool`,
//! whose parameter map is produced identically from a URL plus `user_env` /
//! `pass_env` / `ssl_mode` options. Only the default port and the label used in
//! error messages differ between backends — factor the URL → param-map parse
//! here so the two providers cannot drift.

use anyhow::{Context, Result};
use secrecy::SecretString;
use std::collections::HashMap;

/// Parse a MySQL wire-protocol connection string into the parameter map
/// expected by `MySQLConnectionPool`.
///
/// * `default_port` — applied when the URL has no explicit port (e.g. `3306`
///   for MySQL, `2881` for SeekDB / OceanBase).
/// * `label` — appears in error messages ("MySQL", "SeekDB", ...) so
///   misconfigurations surface with the backend name the user recognises.
///
/// Credentials must come from `user_env` / `pass_env` options, not from the
/// URL — embedded `username:password@host` is ignored (the caller may surface
/// a warning if it detects them).
pub fn parse_mysql_wire_connection_params(
    connection_string: &str,
    options: Option<&HashMap<String, String>>,
    default_port: u16,
    label: &str,
) -> Result<HashMap<String, SecretString>> {
    let url = url::Url::parse(connection_string)
        .with_context(|| format!("Invalid {label} connection string: {connection_string}"))?;

    // Credentials embedded in the URL (`mysql://user:pass@host/db`) are
    // silently ignored by the pool — it only picks up `user` / `pass` from
    // the param map. Surface a warning so a common mistake doesn't manifest
    // as a confusing "access denied" later.
    if !url.username().is_empty() || url.password().is_some() {
        tracing::warn!(
            "{label} connection string contains embedded credentials; these are ignored. \
             Use 'user_env' and 'pass_env' options instead."
        );
    }

    let mut params: HashMap<String, SecretString> = HashMap::new();

    if let Some(host) = url.host_str() {
        params.insert(
            "host".to_string(),
            SecretString::new(host.to_string().into_boxed_str()),
        );
    }

    let port = url.port().unwrap_or(default_port);
    params.insert(
        "tcp_port".to_string(),
        SecretString::new(port.to_string().into_boxed_str()),
    );

    let db_name = url
        .path()
        .trim_start_matches('/')
        .split('/')
        .next()
        .unwrap_or("");

    if !db_name.is_empty() {
        params.insert(
            "db".to_string(),
            SecretString::new(db_name.to_string().into_boxed_str()),
        );
    }

    if let Some(opts) = options {
        if let Some(user_env) = opts.get("user_env") {
            let username = std::env::var(user_env).with_context(|| {
                format!("Environment variable '{user_env}' not found for {label} user")
            })?;
            params.insert(
                "user".to_string(),
                SecretString::new(username.into_boxed_str()),
            );
        }

        if let Some(pass_env) = opts.get("pass_env") {
            let password = std::env::var(pass_env).with_context(|| {
                format!("Environment variable '{pass_env}' not found for {label} password")
            })?;
            params.insert(
                "pass".to_string(),
                SecretString::new(password.into_boxed_str()),
            );
        }
    }

    let ssl_mode = options
        .and_then(|opts| opts.get("ssl_mode"))
        .map(|s| s.to_lowercase())
        .unwrap_or_else(|| {
            tracing::debug!(
                "{label} SSL mode not specified, defaulting to 'disabled' for local development"
            );
            "disabled".to_string()
        });

    params.insert(
        "sslmode".to_string(),
        SecretString::new(ssl_mode.into_boxed_str()),
    );

    Ok(params)
}
