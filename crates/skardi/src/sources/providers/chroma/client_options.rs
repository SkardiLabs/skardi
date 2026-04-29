//! Translate a Skardi `connection_string` + `options` HashMap into a
//! `chroma::ChromaHttpClientOptions`.
//!
//! Connection string is the Chroma server endpoint URL (e.g.
//! `http://localhost:8000` or `https://api.trychroma.com`). Options:
//!
//! | key | purpose | default |
//! |-----|---------|---------|
//! | `collection` | **required** Chroma collection name to register as a table | — |
//! | `tenant` | tenant id | `default_tenant` |
//! | `database` | database name | `default_database` |
//! | `auth_token_env` | env-var name holding the auth token; presence enables auth | none |
//! | `auth_header_name` | HTTP header name to send the token in | `x-chroma-token` |

use std::collections::HashMap;

use anyhow::{Context, Result, anyhow};
use chroma::client::{ChromaAuthMethod, ChromaHttpClientOptions};
use reqwest::Url;
use reqwest::header::{HeaderName, HeaderValue};

const DEFAULT_TENANT: &str = "default_tenant";
const DEFAULT_DATABASE: &str = "default_database";
const DEFAULT_AUTH_HEADER: &str = "x-chroma-token";

pub struct ChromaConnection {
    pub options: ChromaHttpClientOptions,
    pub collection: String,
}

pub fn parse_connection(
    connection_string: &str,
    options: Option<&HashMap<String, String>>,
) -> Result<ChromaConnection> {
    let endpoint = Url::parse(connection_string).with_context(|| {
        format!("chroma: invalid endpoint URL '{connection_string}' — expected http(s)://host:port")
    })?;

    let opts = options;

    let collection = opts
        .and_then(|m| m.get("collection"))
        .ok_or_else(|| anyhow!("chroma: missing required option 'collection'"))?
        .clone();

    let tenant_id = opts
        .and_then(|m| m.get("tenant"))
        .cloned()
        .unwrap_or_else(|| DEFAULT_TENANT.to_string());
    let database_name = opts
        .and_then(|m| m.get("database"))
        .cloned()
        .unwrap_or_else(|| DEFAULT_DATABASE.to_string());

    let auth_method = match opts.and_then(|m| m.get("auth_token_env")) {
        None => ChromaAuthMethod::None,
        Some(env_name) => {
            let token = std::env::var(env_name).with_context(|| {
                format!("chroma: auth_token_env '{env_name}' is not set in the environment")
            })?;
            let header_name = opts
                .and_then(|m| m.get("auth_header_name"))
                .map(String::as_str)
                .unwrap_or(DEFAULT_AUTH_HEADER);
            let header = HeaderName::try_from(header_name)
                .with_context(|| format!("chroma: invalid auth_header_name '{header_name}'"))?;
            let value = HeaderValue::try_from(token.as_str())
                .context("chroma: auth token is not a valid HTTP header value")?;
            ChromaAuthMethod::HeaderAuth { header, value }
        }
    };

    Ok(ChromaConnection {
        options: ChromaHttpClientOptions {
            endpoint,
            auth_method,
            retry_options: Default::default(),
            tenant_id: Some(tenant_id),
            database_name: Some(database_name),
        },
        collection,
    })
}
